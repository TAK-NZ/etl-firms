
import { Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import AdmZip from 'adm-zip';
import { DOMParser } from '@xmldom/xmldom';

const Environment = Type.Object({
    MAP_KEY: Type.Optional(Type.String({
        description: 'NASA FIRMS Map Key — no longer used (CSV sources removed). Kept for backward compatibility.'
    })),
    BBOX: Type.String({
        default: '-47.3,166.3,-34.4,178.6',
        description: 'Bounding box coordinates (minLat,minLon,maxLat,maxLon)'
    }),
    MIN_CONFIDENCE: Type.Number({
        default: 50,
        description: 'Minimum confidence percentage (0-100) for fire detections'
    }),
    MIN_FRP: Type.Number({
        default: 20,
        description: 'Minimum Fire Radiative Power (MW) for fire detections'
    }),
    SHOW_FOOTPRINT: Type.Boolean({
        default: false,
        description: 'Show fire pixel footprint as a square (375m for VIIRS, 1km for MODIS)'
    }),
    FIRE_SEASON_AWARE: Type.Boolean({
        default: false,
        description: 'Enable FENZ fire season lookups to annotate detections. Uses public ArcGIS APIs (no key required).'
    }),
    FIRE_SEASON_CACHE_HOURS: Type.Number({
        default: 6,
        description: 'How long (in hours) to cache FENZ fire season results across invocations. Cached results are stored in ephemeral state and reused on cold starts to reduce API load.'
    })
});

interface ClusterResult {
    corroborated: boolean;
    clusterSize: number;
    satellites: string[];
}

interface FireSeasonResult {
    season: string;
    zone: string;
    district: string;
    onDocLand: boolean;
    section52FireLandMgmt: boolean | null;
}

// Persisted across Lambda invocations. Successful fire season lookups are cached for
// FIRE_SEASON_CACHE_HOURS (configurable, default 6 hours) to avoid hammering the FENZ
// ArcGIS APIs on every 20-second run.
const EphemeralSchema = Type.Object({
    fireSeasonCache: Type.Optional(Type.Record(Type.String(), Type.Object({
        season: Type.String(),
        zone: Type.String(),
        district: Type.String(),
        onDocLand: Type.Boolean(),
        section52FireLandMgmt: Type.Union([Type.Boolean(), Type.Null()]),
        cachedAt: Type.String()
    })))
});

const FireDetectionSchema = Type.Object({
    satellite: Type.String({ description: 'Satellite name (MODIS, VIIRS S-NPP, VIIRS NOAA-20, VIIRS NOAA-21)' }),
    time_since_detection: Type.String({ description: 'Time since detection in hours (< 1, 1-3, 3-6, 6-12, 12-24)' }),
    acq_date: Type.String({ description: 'Acquisition date (YYYY-MM-DD)' }),
    acq_time: Type.String({ description: 'Acquisition time (HHMM)' }),
    acq_datetime: Type.String({ description: 'Acquisition datetime (ISO 8601)' }),
    brightness: Type.Number({ description: 'Brightness temperature (Kelvin)' }),
    confidence: Type.Number({ description: 'Confidence percentage (0-100)' }),
    brightness_2: Type.Number({ description: 'Secondary brightness temperature (Kelvin)' }),
    frp: Type.Number({ description: 'Fire Radiative Power (MW)' }),
    daynight: Type.String({ description: 'Day or Night detection (D/N)' }),
    version: Type.String({ description: 'Data version' }),
    latitude: Type.Number({ description: 'Latitude' }),
    longitude: Type.Number({ description: 'Longitude' }),
    cluster_size: Type.Optional(Type.Number({ description: 'Number of detections within 500m/2h' })),
    fire_season: Type.Optional(Type.String({ description: 'FENZ fire season status (Open, Restricted, Prohibited) (when FIRE_SEASON_AWARE is enabled)' }))
});

interface FireData {
    latitude: number;
    longitude: number;
    brightness: number;
    scan: number;
    track: number;
    acq_date: string;
    acq_time: string;
    acq_datetime: string;
    satellite: string;
    confidence: number;
    version: string;
    brightness_2: number;
    frp: number;
    daynight: string;
}

export default class Task extends ETL {
    static name = 'etl-firms';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    private static readonly FIRE_ICON = 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:Incidents/INC.35.Fire.png';
    private static readonly METERS_PER_DEGREE = 111320;
    private static readonly CLUSTER_RADIUS_M = 500;
    private static readonly CLUSTER_TIME_HOURS = 2;

    // In-memory cache for the current container lifetime.
    // Pre-populated from ephemeral at the start of each control() run when FIRE_SEASON_AWARE is set.
    private fireSeasonCache = new Map<string, FireSeasonResult | null>();
    private fireSeasonCacheTime = new Map<string, number>(); // epoch ms when each entry was fetched

    private createFootprintPolygon(lat: number, lon: number, pixelSize: number): [number, number][] {
        const halfSize = (pixelSize / 2) / Task.METERS_PER_DEGREE;
        const halfSizeLon = halfSize / Math.cos(lat * Math.PI / 180);

        return [
            [lon - halfSizeLon, lat - halfSize],
            [lon + halfSizeLon, lat - halfSize],
            [lon + halfSizeLon, lat + halfSize],
            [lon - halfSizeLon, lat + halfSize],
            [lon - halfSizeLon, lat - halfSize]
        ];
    }

    private async queryFireSeason(lon: number, lat: number): Promise<FireSeasonResult | null> {
        const key = `${lon.toFixed(1)}_${lat.toFixed(1)}`;
        if (this.fireSeasonCache.has(key)) return this.fireSeasonCache.get(key)!;

        try {
            const baseParams = `f=json&geometry=${lon},${lat}&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&returnGeometry=false`;

            const [seasonResult, docResult, s52Result] = await Promise.all([
                fetch(`https://utility.arcgis.com/usrsvcs/servers/b381ba9bdd4046c2b71a6b1a1e39da78/rest/services/FENZ/MapServer/27/query?${baseParams}&outFields=Name,SEASON,Region,District,Zone_`)
                    .then(async r => ({ ok: r.ok, status: r.status, data: r.ok ? await r.json() as { features?: Array<{ attributes?: Record<string, unknown> }> } : null })),
                fetch(`https://utility.arcgis.com/usrsvcs/servers/e6865ced067f4234b047732b4d38a711/rest/services/FENZ_PCL/FeatureServer/0/query?${baseParams}&outFields=OBJECTID`)
                    .then(async r => ({ ok: r.ok, data: r.ok ? await r.json() as { features?: unknown[] } : null })),
                fetch(`https://services1.arcgis.com/dcVyucRsXCQAi3GL/arcgis/rest/services/Section_52_Localities_v2_view/FeatureServer/0/query?${baseParams}&outFields=Fire_Land_Management`)
                    .then(async r => ({ ok: r.ok, data: r.ok ? await r.json() as { features?: Array<{ attributes?: { Fire_Land_Management?: number } }> } : null }))
            ]);

            if (!seasonResult.ok) {
                console.warn(`FENZ fire season API returned ${seasonResult.status}`);
                this.fireSeasonCache.set(key, null);
                return null;
            }

            const seasonAttr = seasonResult.data?.features?.[0]?.attributes;
            if (!seasonAttr) {
                this.fireSeasonCache.set(key, null);
                return null;
            }

            let season = String(seasonAttr.SEASON || 'Unknown');
            if (season === 'Defence' || season === 'Prohibition in open air') season = 'Prohibited';

            const onDocLand = docResult.ok && (docResult.data?.features?.length || 0) > 0;
            if (onDocLand && season === 'Open') season = 'Restricted';

            let section52FireLandMgmt: boolean | null = null;
            if (s52Result.ok) {
                const s52Attr = s52Result.data?.features?.[0]?.attributes;
                if (s52Attr && s52Attr.Fire_Land_Management !== undefined) {
                    section52FireLandMgmt = s52Attr.Fire_Land_Management === 1;
                }
            }

            const result: FireSeasonResult = {
                season,
                zone: String(seasonAttr.Zone_ || ''),
                district: String(seasonAttr.District || ''),
                onDocLand,
                section52FireLandMgmt
            };
            this.fireSeasonCache.set(key, result);
            this.fireSeasonCacheTime.set(key, Date.now());
            return result;
        } catch (err) {
            console.warn('FENZ fire season query failed:', err);
            this.fireSeasonCache.set(key, null);
            return null;
        }
    }

    private findCluster(fire: FireData, allFires: FireData[]): ClusterResult {
        const satellites = new Set<string>();
        satellites.add(fire.satellite);
        let neighbourCount = 0;
        const fireTime = new Date(`${fire.acq_date}T${String(fire.acq_time).padStart(4, '0').slice(0, 2)}:${String(fire.acq_time).padStart(4, '0').slice(2)}:00Z`).getTime();

        for (const other of allFires) {
            if (other === fire) continue;

            const otherTime = new Date(`${other.acq_date}T${String(other.acq_time).padStart(4, '0').slice(0, 2)}:${String(other.acq_time).padStart(4, '0').slice(2)}:00Z`).getTime();
            if (Math.abs(fireTime - otherTime) > Task.CLUSTER_TIME_HOURS * 3600000) continue;

            const dLat = (fire.latitude - other.latitude) * Task.METERS_PER_DEGREE;
            const dLon = (fire.longitude - other.longitude) * Task.METERS_PER_DEGREE * Math.cos(fire.latitude * Math.PI / 180);
            if (Math.sqrt(dLat * dLat + dLon * dLon) <= Task.CLUSTER_RADIUS_M) {
                satellites.add(other.satellite);
                neighbourCount++;
            }
        }

        return { corroborated: neighbourCount > 0, clusterSize: neighbourCount + 1, satellites: Array.from(satellites) };
    }

    private parseKML(kmlContent: string, source: string, bbox: { minLat: number; minLon: number; maxLat: number; maxLon: number }): FireData[] {
        const fires: FireData[] = [];
        const parser = new DOMParser();
        const doc = parser.parseFromString(kmlContent, 'text/xml');
        const placemarks = doc.getElementsByTagName('Placemark');

        const satelliteMap: Record<string, string> = {
            'MODIS_C6.1': 'MODIS',
            'VIIRS_SNPP': 'VIIRS S-NPP',
            'VIIRS_NOAA20': 'VIIRS NOAA-20',
            'VIIRS_NOAA21': 'VIIRS NOAA-21'
        };

        console.log(`Total placemarks in ${source}:`, placemarks.length);
        let nzCount = 0;

        for (let i = 0; i < placemarks.length; i++) {
            const placemark = placemarks[i];
            const name = placemark.getElementsByTagName('name')[0]?.textContent || '';

            // Only process centroid points, skip footprint polygons
            if (!name.includes('Fire Detection Centroid')) continue;

            const description = placemark.getElementsByTagName('description')[0]?.textContent || '';
            const coordinates = placemark.getElementsByTagName('coordinates')[0]?.textContent?.trim();

            if (!coordinates) continue;

            const [lon, lat] = coordinates.split(',').map(Number);

            // Filter for New Zealand coordinates
            if (lon < bbox.minLon || lon > bbox.maxLon || lat < bbox.minLat || lat > bbox.maxLat) continue;
            nzCount++;

            // Extract data from KML description
            const detectionTimeMatch = description.match(/Detection Time.*?(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2})/);
            const sensorMatch = description.match(/<b>Sensor: <\/b>\s*([^<]+)/);
            const confidenceMatch = description.match(/<b>Confidence[^:]*: <\/b>\s*([^<]+)/);
            const frpMatch = description.match(/<b>FRP: <\/b>\s*([\d.]+)/);
            const brightnessMatch = description.match(/<b>Brightness: <\/b>\s*([\d.]+)/);
            const dayNightMatch = description.match(/<b>Day\/Night: <\/b>\s*([^<]+)/);
            const scanMatch = description.match(/<b>Scan: <\/b>\s*([\d.]+)/);
            const trackMatch = description.match(/<b>Track: <\/b>\s*([\d.]+)/);

            // Parse detection time
            let acq_date = new Date().toISOString().split('T')[0];
            let acq_time = '1200';
            let acq_datetime = acq_date;

            if (detectionTimeMatch) {
                acq_date = detectionTimeMatch[1];
                acq_time = detectionTimeMatch[2] + detectionTimeMatch[3];
                acq_datetime = `${acq_date}T${detectionTimeMatch[2]}:${detectionTimeMatch[3]}:00Z`;
            }

            // Parse confidence (handle both percentage and text)
            let confidence = 60;
            if (confidenceMatch) {
                const confStr = confidenceMatch[1].trim();
                if (confStr.includes('%')) {
                    confidence = parseInt(confStr.replace('%', ''));
                } else if (confStr.toLowerCase().includes('high')) {
                    confidence = 80;
                } else if (confStr.toLowerCase().includes('nominal')) {
                    confidence = 60;
                } else if (confStr.toLowerCase().includes('low')) {
                    confidence = 40;
                }
            }

            // Parse satellite name
            let satellite = satelliteMap[source] || source;
            if (sensorMatch) {
                const sensor = sensorMatch[1].trim();
                if (sensor.includes('MODIS')) {
                    satellite = 'MODIS';
                } else if (sensor.includes('VIIRS')) {
                    if (sensor.includes('Suomi-NPP')) satellite = 'VIIRS S-NPP';
                    else if (sensor.includes('NOAA-20')) satellite = 'VIIRS NOAA-20';
                    else if (sensor.includes('NOAA-21')) satellite = 'VIIRS NOAA-21';
                    else satellite = 'VIIRS';
                }
            }

            const fire: FireData = {
                latitude: lat,
                longitude: lon,
                satellite: satellite,
                acq_date: acq_date,
                acq_time: acq_time,
                acq_datetime: acq_datetime,
                scan: scanMatch ? parseFloat(scanMatch[1]) : 0,
                track: trackMatch ? parseFloat(trackMatch[1]) : 0,
                version: '2.0',
                daynight: dayNightMatch ? (dayNightMatch[1].trim().toLowerCase().includes('day') ? 'D' : 'N') : 'D',
                brightness: brightnessMatch ? parseFloat(brightnessMatch[1]) : 0,
                brightness_2: brightnessMatch ? parseFloat(brightnessMatch[1]) : 0,
                confidence: confidence,
                frp: frpMatch ? parseFloat(frpMatch[1]) : 0
            };

            fires.push(fire);
        }

        console.log(`NZ coordinates found in ${source}:`, nzCount, `fires parsed:`, fires.length);
        return fires;
    }

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Environment;
            } else {
                return FireDetectionSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        // Load ephemeral state and seed the in-memory fire season cache with fresh entries.
        // This means a cold-started container can reuse lookups from the previous run rather
        // than re-querying every unique location on the first invocation after a restart.
        const ephemeral = await this.ephemeral(EphemeralSchema);
        if (!ephemeral.fireSeasonCache) ephemeral.fireSeasonCache = {};

        if (env.FIRE_SEASON_AWARE) {
            const cutoff = Date.now() - env.FIRE_SEASON_CACHE_HOURS * 60 * 60 * 1000;
            let seeded = 0;
            for (const [key, entry] of Object.entries(ephemeral.fireSeasonCache)) {
                const cachedAt = new Date(entry.cachedAt).getTime();
                if (cachedAt > cutoff && !this.fireSeasonCache.has(key)) {
                    this.fireSeasonCache.set(key, {
                        season: entry.season,
                        zone: entry.zone,
                        district: entry.district,
                        onDocLand: entry.onDocLand,
                        section52FireLandMgmt: entry.section52FireLandMgmt
                    });
                    this.fireSeasonCacheTime.set(key, cachedAt);
                    seeded++;
                }
            }
            if (seeded > 0) console.log(`FENZ cache: seeded ${seeded} entries from ephemeral`);
        }

        let allFires: FireData[] = [];

        const [minLat, minLon, maxLat, maxLon] = env.BBOX.split(',').map(Number);
        const bbox = { minLat, minLon, maxLat, maxLon };

        // Fetch data from KML sources (public, no API key required)
        const kmlSources = [
            { name: 'MODIS_C6.1', url: 'https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/australia_newzealand/24h/c6.1/FirespotArea_australia_newzealand_c6.1_24h.kmz' },
            { name: 'VIIRS_SNPP', url: 'https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/australia_newzealand/24h/suomi-npp-viirs-c2/FirespotArea_australia_newzealand_suomi-npp-viirs-c2_24h.kmz' },
            { name: 'VIIRS_NOAA20', url: 'https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/australia_newzealand/24h/noaa-20-viirs-c2/FirespotArea_australia_newzealand_noaa-20-viirs-c2_24h.kmz' },
            { name: 'VIIRS_NOAA21', url: 'https://firms.modaps.eosdis.nasa.gov/api/kml_fire_footprints/australia_newzealand/24h/noaa-21-viirs-c2/FirespotArea_australia_newzealand_noaa-21-viirs-c2_24h.kmz' }
        ];

        for (const kmlSource of kmlSources) {
            try {
                const kmlRes = await fetch(kmlSource.url);

                if (!kmlRes.ok) {
                    console.warn(`KML API returned ${kmlRes.status} for ${kmlSource.name}: ${kmlRes.statusText}`);
                    continue;
                }

                const kmzBuffer = await kmlRes.arrayBuffer();
                const zip = new (AdmZip as unknown as new (buffer: Buffer) => AdmZip)(Buffer.from(kmzBuffer));
                const entries = zip.getEntries();

                console.log(`KMZ entries for ${kmlSource.name}:`, entries.map((e: AdmZip.IZipEntry) => e.entryName));
                for (const entry of entries) {
                    if (entry.entryName.endsWith('.kml')) {
                        const kmlContent = entry.getData().toString('utf8');
                        console.log(`KML content length for ${kmlSource.name}:`, kmlContent.length);
                        const fires = this.parseKML(kmlContent, kmlSource.name, bbox);
                        allFires = allFires.concat(fires);
                        console.log(`Found ${fires.length} fire detections from ${kmlSource.name} KML`);
                        break;
                    }
                }
            } catch (error) {
                console.error(`Error fetching ${kmlSource.name} KML:`, error);
            }
        }

        // Deduplicate fires based on coordinates and time (use 3 decimal places for coordinate matching)
        const uniqueFires = new Map<string, FireData>();
        let duplicatesFound = 0;
        for (const fire of allFires) {
            const key = `${fire.latitude.toFixed(3)}_${fire.longitude.toFixed(3)}_${fire.acq_date}_${String(fire.acq_time).padStart(4, '0')}`;
            if (!uniqueFires.has(key)) {
                uniqueFires.set(key, fire);
            } else {
                duplicatesFound++;
                // Keep the fire with higher confidence if duplicate found
                const existing = uniqueFires.get(key)!;
                if (fire.confidence > existing.confidence) {
                    uniqueFires.set(key, fire);
                }
            }
        }
        console.log(`Removed ${duplicatesFound} duplicate detections`);
        allFires = Array.from(uniqueFires.values());

        console.log(`Found ${allFires.length} total fire detections after deduplication`);

        // Compute clusters across the full batch
        const clusterMap = new Map<FireData, ClusterResult>();
        for (const fire of allFires) {
            clusterMap.set(fire, this.findCluster(fire, allFires));
        }

        const fc = {
            type: 'FeatureCollection' as const,
            features: [] as Array<{
                id: string;
                type: 'Feature';
                properties: Record<string, unknown>;
                geometry: { type: 'Point'; coordinates: [number, number] } | { type: 'Polygon'; coordinates: [number, number][][] };
            }>
        };

        const processedFeatures = new Map<string, typeof fc.features[number]>();

        for (const fire of allFires) {
            // Filter by minimum confidence and FRP
            if (fire.confidence < env.MIN_CONFIDENCE || fire.frp < env.MIN_FRP) {
                continue;
            }

            try {
                const fireId = `${fire.satellite}_${fire.acq_date}_${String(fire.acq_time).padStart(4, '0')}_${fire.latitude.toFixed(3)}_${fire.longitude.toFixed(3)}`;

                if (processedFeatures.has(fireId)) continue;

                const timeStr = String(fire.acq_time).padStart(4, '0');
                const hours = timeStr.slice(0, 2);
                const minutes = timeStr.slice(2, 4);
                const acqDateTimeUTC = `${fire.acq_date}T${hours}:${minutes}:00Z`;
                const acqTime = new Date(acqDateTimeUTC);
                const brightness2 = fire.brightness_2 || fire.brightness;

                const now = new Date();
                const hoursSince = (now.getTime() - acqTime.getTime()) / (1000 * 60 * 60);
                const timeSinceCategory = hoursSince < 1 ? '< 1' : hoursSince < 3 ? '1-3' : hoursSince < 6 ? '3-6' : hoursSince < 12 ? '6-12' : '12-24';

                const cluster = clusterMap.get(fire) || { corroborated: false, clusterSize: 1, satellites: [fire.satellite] };

                let fireSeason: FireSeasonResult | null = null;
                if (env.FIRE_SEASON_AWARE) {
                    fireSeason = await this.queryFireSeason(fire.longitude, fire.latitude);
                }

                // Build remarks
                const remarkLines = [
                    `Satellite: ${fire.satellite}`,
                    `Time since detection: ${timeSinceCategory} hours`,
                    `Acquisition (UTC): ${acqTime.toISOString().replace('T', ' ').replace('Z', '')}`,
                    `Acquisition (NZT): ${acqTime.toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' })}`,
                    `Confidence: ${fire.confidence}%`,
                    `Brightness temperature: ${fire.brightness}`,
                    `Fire Radiative Power (FRP): ${fire.frp}`
                ];

                let callsign = `FIRMS Detection - FRP: ${fire.frp}`;

                if (cluster.corroborated) {
                    remarkLines.push(`Corroborated: ${cluster.clusterSize} detections within 500m/2h (${cluster.satellites.join(', ')})`);
                    callsign += ` [${cluster.clusterSize}x]`;
                }

                if (fireSeason) {
                    const fs = fireSeason;
                    remarkLines.push(`Fire Season: ${fs.season} (${fs.zone}, ${fs.district})`);
                    if (fs.onDocLand) remarkLines.push('DoC Conservation Land: Yes');
                    if (fs.section52FireLandMgmt !== null) {
                        remarkLines.push(`Section 52: Fire for land management — ${fs.section52FireLandMgmt ? 'allowed' : 'prohibited'}`);
                    }
                } else if (env.FIRE_SEASON_AWARE) {
                    remarkLines.push('Fire Season: Lookup failed');
                }

                const remarks = remarkLines.join('\n');

                const metadata: Record<string, unknown> = {
                    satellite: fire.satellite,
                    time_since_detection: timeSinceCategory,
                    acq_date: fire.acq_date,
                    acq_time: fire.acq_time,
                    acq_datetime: acqDateTimeUTC,
                    brightness: fire.brightness,
                    confidence: fire.confidence,
                    brightness_2: brightness2,
                    frp: fire.frp,
                    daynight: fire.daynight,
                    version: fire.version,
                    cluster_size: cluster.clusterSize
                };

                if (fireSeason) metadata.fire_season = fireSeason.season;

                const feature = {
                    id: fireId,
                    type: 'Feature' as const,
                    properties: {
                        callsign,
                        type: 'a-f-X-i',
                        time: acqTime.toISOString(),
                        start: acqTime.toISOString(),
                        icon: Task.FIRE_ICON,
                        metadata,
                        remarks,
                        archived: false
                    },
                    geometry: {
                        type: 'Point' as const,
                        coordinates: [fire.longitude, fire.latitude] as [number, number]
                    }
                };

                processedFeatures.set(fireId, feature);
                fc.features.push(feature);

                if (env.SHOW_FOOTPRINT) {
                    const pixelSize = fire.satellite.includes('VIIRS') ? 375 : 1000;
                    fc.features.push({
                        id: `${fireId}_footprint`,
                        type: 'Feature' as const,
                        properties: {
                            callsign: `FIRMS Footprint - ${fire.satellite}`,
                            type: 'u-d-f',
                            time: acqTime.toISOString(),
                            start: acqTime.toISOString(),
                            metadata: { ...metadata, pixel_size: pixelSize, parent_id: fireId },
                            remarks,
                            archived: false
                        },
                        geometry: {
                            type: 'Polygon' as const,
                            coordinates: [this.createFootprintPolygon(fire.latitude, fire.longitude, pixelSize)]
                        }
                    });
                }
            } catch (error) {
                console.error(`Error processing fire detection:`, error);
            }
        }

        // Persist successful fire season lookups back to ephemeral so the next cold-started
        // container can reuse them without re-querying. Failures (null) are intentionally
        // excluded so they are retried after a restart. Entries older than the TTL are pruned.
        if (env.FIRE_SEASON_AWARE) {
            const cutoff = Date.now() - env.FIRE_SEASON_CACHE_HOURS * 60 * 60 * 1000;
            const updatedCache: typeof ephemeral.fireSeasonCache = {};
            for (const [key, result] of this.fireSeasonCache.entries()) {
                if (result === null) continue;
                const cachedAt = this.fireSeasonCacheTime.get(key);
                if (cachedAt && cachedAt > cutoff) {
                    updatedCache[key] = { ...result, cachedAt: new Date(cachedAt).toISOString() };
                }
            }
            ephemeral.fireSeasonCache = updatedCache;
            await this.setEphemeral(ephemeral);
            console.log(`FENZ cache: persisted ${Object.keys(updatedCache).length} entries to ephemeral`);
        }

        console.log(`ok - obtained ${fc.features.length} FIRMS fire features`);
        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}
