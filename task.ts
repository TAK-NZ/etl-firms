
import { Type, TSchema, Static } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import AdmZip from 'adm-zip';
import { DOMParser } from '@xmldom/xmldom';

const Environment = Type.Object({
    MAP_KEY: Type.String({
        description: 'NASA FIRMS Map Key from https://firms.modaps.eosdis.nasa.gov/mapserver/wfs-info/'
    }),
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
    LAND_COVER_MASKING: Type.Boolean({
        default: false,
        description: 'Enable land cover masking using LCDB v6.0 to classify fire detections. Requires LRIS_API_KEY.'
    }),
    LRIS_API_KEY: Type.Optional(Type.String({
        description: 'LRIS (Manaaki Whenua) API key for land cover lookups. Get one free at https://lris.scinfo.org.nz/. If not provided, land cover masking is disabled and all detections pass through unfiltered.'
    })),
    FRP_THRESHOLD_HIGH: Type.Number({
        default: 10,
        description: 'Minimum FRP (MW) for High risk land cover classes (e.g. Broadleaved Indigenous Hardwoods)'
    }),
    FRP_THRESHOLD_MEDIUM: Type.Number({
        default: 25,
        description: 'Minimum FRP (MW) for Medium risk land cover classes (e.g. Manuka/Kanuka) during daytime'
    }),
    FRP_THRESHOLD_LOW_SCRUB: Type.Number({
        default: 30,
        description: 'Minimum FRP (MW) for Low risk scrub classes (e.g. Gorse and/or Broom)'
    }),
    FRP_THRESHOLD_LOW_GRASS: Type.Number({
        default: 40,
        description: 'Minimum FRP (MW) for Low risk grassland classes (e.g. High Producing Exotic Grassland) during daytime'
    }),
    FILTER_URBAN_HEAT: Type.Boolean({
        default: false,
        description: 'Filter out detections on urban land cover (Built-up Area, Transport Infrastructure, Surface Mine or Dump). Requires LAND_COVER_MASKING.'
    }),
    FIRE_SEASON_AWARE: Type.Boolean({
        default: false,
        description: 'Enable FENZ fire season lookups to adjust detection priority. Uses public ArcGIS APIs (no key required).'
    })
});

type RiskLevel = 'Critical' | 'High' | 'Medium' | 'Low-Grass' | 'Low-Scrub' | 'Ignore' | 'Pass-through';

const RISK_PRIORITY: Record<RiskLevel, number> = {
    'Critical': 6, 'High': 5, 'Medium': 4, 'Low-Scrub': 3, 'Low-Grass': 2, 'Ignore': 1, 'Pass-through': 0
};

const LAND_COVER_RISK: Record<string, RiskLevel> = {
    'Indigenous Forest': 'Critical',
    'Exotic Forest': 'Critical',
    'Broadleaved Indigenous Hardwoods': 'High',
    'Deciduous Hardwoods': 'Medium',
    'Manuka and/or Kanuka': 'Medium',
    'Matagouri or Grey Scrub': 'Medium',
    'Sub Alpine Shrubland': 'Medium',
    'Fernland': 'Medium',
    'Flaxland': 'Medium',
    'High Producing Exotic Grassland': 'Low-Grass',
    'Low Producing Grassland': 'Low-Grass',
    'Tall Tussock Grassland': 'Low-Grass',
    'Short-rotation Cropland': 'Low-Grass',
    'Orchard Vineyard and Other Perennial Crops': 'Low-Grass',
    'Gorse and/or Broom': 'Low-Scrub',
    'Forest - Harvested': 'Low-Grass',
    'Built-up Area (settlement)': 'Ignore',
    'Urban Parkland/Open Space': 'Ignore',
    'Transport Infrastructure': 'Ignore',
    'Surface Mine or Dump': 'Ignore',
    'Not land': 'Ignore',
};

interface LandCoverFeature {
    name: string;
    classId: number;
    risk: RiskLevel;
}

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

interface Assessment {
    landCover: {
        primaryClass: string;
        classId: number;
        riskLevel: string;
        passThrough: boolean;
        allClasses: LandCoverFeature[];
    } | null;
    fireSeason: FireSeasonResult | null;
    cluster: ClusterResult;
    timestamp: string;
}

const EphemeralSchema = Type.Object({
    assessments: Type.Optional(Type.Record(Type.String(), Type.Object({
        landCover: Type.Optional(Type.Object({
            primaryClass: Type.String(),
            classId: Type.Number(),
            riskLevel: Type.String(),
            passThrough: Type.Boolean(),
            allClasses: Type.Array(Type.Object({
                name: Type.String(),
                classId: Type.Number(),
                risk: Type.String()
            }))
        })),
        fireSeason: Type.Optional(Type.Object({
            season: Type.String(),
            zone: Type.String(),
            district: Type.String(),
            onDocLand: Type.Boolean(),
            section52FireLandMgmt: Type.Union([Type.Boolean(), Type.Null()])
        })),
        cluster: Type.Optional(Type.Object({
            corroborated: Type.Boolean(),
            clusterSize: Type.Number(),
            satellites: Type.Array(Type.String())
        })),
        timestamp: Type.String()
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
    land_cover: Type.Optional(Type.String({ description: 'Highest-risk LCDB v6.0 land cover class name (when LAND_COVER_MASKING is enabled)' })),
    land_cover_risk: Type.Optional(Type.String({ description: 'Risk level (Critical, High, Medium, Low-Grass, Low-Scrub, Ignore, Pass-through)' })),
    cluster_size: Type.Optional(Type.Number({ description: 'Number of detections within 500m/2h (when LAND_COVER_MASKING is enabled)' })),
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
    satellite: string;
    confidence: number;
    version: string;
    brightness_2: number;
    frp: number;
    daynight: string;
    acq_datetime: string;
}

export default class Task extends ETL {
    static name = 'etl-firms';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    private static readonly FIRE_ICON = 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:Incidents/INC.35.Fire.png';
    private static readonly METERS_PER_DEGREE = 111320;
    private static readonly CLUSTER_RADIUS_M = 500;
    private static readonly CLUSTER_TIME_HOURS = 2;
    private static readonly FIRE_SEASON_MULTIPLIER = 1.5;

    private landCoverCache = new Map<string, LandCoverFeature[] | null>();
    private fireSeasonCache = new Map<string, FireSeasonResult | null>();

    private computeFootprintBbox(lat: number, lon: number, pixelSize: number): { lonMin: number; latMin: number; lonMax: number; latMax: number } {
        const halfSize = (pixelSize / 2) / Task.METERS_PER_DEGREE;
        const halfSizeLon = halfSize / Math.cos(lat * Math.PI / 180);
        return {
            lonMin: lon - halfSizeLon,
            latMin: lat - halfSize,
            lonMax: lon + halfSizeLon,
            latMax: lat + halfSize
        };
    }

    private createFootprintPolygon(lat: number, lon: number, pixelSize: number): [number, number][] {
        // Convert pixel size from meters to degrees (approximate)
        const metersPerDegree = 111320; // at equator
        const halfSize = (pixelSize / 2) / metersPerDegree;
        const halfSizeLon = halfSize / Math.cos(lat * Math.PI / 180);

        return [
            [lon - halfSizeLon, lat - halfSize],
            [lon + halfSizeLon, lat - halfSize],
            [lon + halfSizeLon, lat + halfSize],
            [lon - halfSizeLon, lat + halfSize],
            [lon - halfSizeLon, lat - halfSize]
        ];
    }

    private cacheKey(bbox: { lonMin: number; latMin: number; lonMax: number; latMax: number }): string {
        return `${bbox.lonMin.toFixed(2)}_${bbox.latMin.toFixed(2)}_${bbox.lonMax.toFixed(2)}_${bbox.latMax.toFixed(2)}`;
    }

    private isNightTimeNZ(acqDateTimeUTC: string): boolean {
        const d = new Date(acqDateTimeUTC);
        const nzHour = parseInt(d.toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland', hour: 'numeric', hour12: false }));
        return nzHour >= 21 || nzHour < 6;
    }

    private async queryLandCover(
        bbox: { lonMin: number; latMin: number; lonMax: number; latMax: number },
        apiKey: string
    ): Promise<LandCoverFeature[] | null> {
        const key = this.cacheKey(bbox);
        if (this.landCoverCache.has(key)) return this.landCoverCache.get(key)!;

        try {
            const cql = `BBOX(GEOMETRY,${bbox.lonMin},${bbox.latMin},${bbox.lonMax},${bbox.latMax},'EPSG:4326')`;
            const url = `https://lris.scinfo.org.nz/services;key=${apiKey}/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=layer-123148&outputFormat=json&srsName=EPSG:4326&propertyName=Name_2023,Class_2023&cql_filter=${encodeURIComponent(cql)}`;
            const res = await fetch(url);
            if (!res.ok) {
                console.warn(`LRIS WFS returned ${res.status}: ${res.statusText}`);
                this.landCoverCache.set(key, null);
                return null;
            }
            const data = await res.json() as { features?: Array<{ properties?: { Name_2023?: string; Class_2023?: number } }> };
            const features: LandCoverFeature[] = (data.features || []).map(f => {
                const name = f.properties?.Name_2023 || 'Unknown';
                const classId = f.properties?.Class_2023 || 0;
                return { name, classId, risk: LAND_COVER_RISK[name] || 'Pass-through' };
            });
            this.landCoverCache.set(key, features);
            return features;
        } catch (err) {
            console.warn('LRIS land cover query failed:', err);
            this.landCoverCache.set(key, null);
            return null;
        }
    }

    private async queryFireSeason(lon: number, lat: number): Promise<FireSeasonResult | null> {
        const key = `${lon.toFixed(1)}_${lat.toFixed(1)}`;
        if (this.fireSeasonCache.has(key)) return this.fireSeasonCache.get(key)!;

        try {
            const baseParams = `f=json&geometry=${lon},${lat}&geometryType=esriGeometryPoint&inSR=4326&spatialRel=esriSpatialRelIntersects&returnGeometry=false`;

            const [seasonRes, docRes, s52Res] = await Promise.all([
                fetch(`https://utility.arcgis.com/usrsvcs/servers/b381ba9bdd4046c2b71a6b1a1e39da78/rest/services/FENZ/MapServer/27/query?${baseParams}&outFields=Name,SEASON,Region,District,Zone_`),
                fetch(`https://utility.arcgis.com/usrsvcs/servers/e6865ced067f4234b047732b4d38a711/rest/services/FENZ_PCL/FeatureServer/0/query?${baseParams}&outFields=OBJECTID`),
                fetch(`https://services1.arcgis.com/dcVyucRsXCQAi3GL/arcgis/rest/services/Section_52_Localities_v2_view/FeatureServer/0/query?${baseParams}&outFields=Fire_Land_Management`)
            ]);

            if (!seasonRes.ok) {
                console.warn(`FENZ fire season API returned ${seasonRes.status}`);
                this.fireSeasonCache.set(key, null);
                return null;
            }

            const seasonData = await seasonRes.json() as { features?: Array<{ attributes?: Record<string, unknown> }> };
            const seasonAttr = seasonData.features?.[0]?.attributes;
            if (!seasonAttr) {
                this.fireSeasonCache.set(key, null);
                return null;
            }

            let season = String(seasonAttr.SEASON || 'Unknown');
            if (season === 'Defence' || season === 'Prohibition in open air') season = 'Prohibited';

            const onDocLand = docRes.ok && ((await docRes.json() as { features?: unknown[] }).features?.length || 0) > 0;
            if (onDocLand && season === 'Open') season = 'Restricted';

            let section52FireLandMgmt: boolean | null = null;
            if (s52Res.ok) {
                const s52Data = await s52Res.json() as { features?: Array<{ attributes?: { Fire_Land_Management?: number } }> };
                const s52Attr = s52Data.features?.[0]?.attributes;
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

    private classifyDetection(
        landCoverFeatures: LandCoverFeature[] | null,
        frp: number,
        acqTimeUTC: string,
        env: Static<typeof Environment>,
        corroboration: ClusterResult,
        fireSeason: FireSeasonResult | null
    ): { passThrough: boolean; riskLevel: RiskLevel; classes: LandCoverFeature[] } {
        if (!landCoverFeatures || landCoverFeatures.length === 0) {
            return { passThrough: true, riskLevel: 'Pass-through', classes: [] };
        }

        // Determine highest risk
        let highest: RiskLevel = 'Pass-through';
        for (const f of landCoverFeatures) {
            if (RISK_PRIORITY[f.risk] > RISK_PRIORITY[highest]) highest = f.risk;
        }

        // Critical: always pass
        if (highest === 'Critical') return { passThrough: true, riskLevel: highest, classes: landCoverFeatures };

        // High: apply threshold, no overrides
        if (highest === 'High') {
            return { passThrough: frp > env.FRP_THRESHOLD_HIGH, riskLevel: highest, classes: landCoverFeatures };
        }

        // Ignore: filter if toggle enabled
        if (highest === 'Ignore') {
            return { passThrough: !env.FILTER_URBAN_HEAT, riskLevel: highest, classes: landCoverFeatures };
        }

        // Medium / Low-Grass / Low-Scrub: check overrides
        const isNight = this.isNightTimeNZ(acqTimeUTC);
        const isCorroborated = corroboration.corroborated;
        const isRestricted = fireSeason && (fireSeason.season === 'Restricted' || fireSeason.season === 'Prohibited');

        if (isNight || isCorroborated || isRestricted) {
            return { passThrough: true, riskLevel: highest, classes: landCoverFeatures };
        }

        // Apply FRP threshold with fire season multiplier
        const seasonMultiplier = (fireSeason && fireSeason.season === 'Open') ? Task.FIRE_SEASON_MULTIPLIER : 1;
        let threshold: number;
        if (highest === 'Medium') threshold = env.FRP_THRESHOLD_MEDIUM * seasonMultiplier;
        else if (highest === 'Low-Scrub') threshold = env.FRP_THRESHOLD_LOW_SCRUB * seasonMultiplier;
        else threshold = env.FRP_THRESHOLD_LOW_GRASS * seasonMultiplier;

        return { passThrough: frp > threshold, riskLevel: highest, classes: landCoverFeatures };
    }

    private parseKML(kmlContent: string, source: string): FireData[] {
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
            if (lon < 166 || lon > 179 || lat < -47 || lat > -34) continue;
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
    
    private parseCSV(csvText: string, source: string): FireData[] {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return [];
        
        const headers = lines[0].split(',');
        const fires: FireData[] = [];
        
        // Map source to satellite name
        const satelliteMap: Record<string, string> = {
            'MODIS_NRT': 'MODIS',
            'VIIRS_SNPP_NRT': 'VIIRS S-NPP',
            'VIIRS_NOAA20_NRT': 'VIIRS NOAA-20',
            'VIIRS_NOAA21_NRT': 'VIIRS NOAA-21'
        };
        
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',');
            if (values.length !== headers.length) continue;
            
            const fire: Record<string, string | number> = {};
            headers.forEach((header, index) => {
                const value = values[index]?.trim();
                if (['latitude', 'longitude', 'brightness', 'scan', 'track', 'brightness_2', 'frp'].includes(header)) {
                    fire[header] = parseFloat(value);
                } else if (header === 'confidence') {
                    // Handle both numeric (MODIS) and letter (VIIRS) confidence values
                    const numValue = parseFloat(value);
                    if (!isNaN(numValue)) {
                        fire[header] = numValue;
                    } else {
                        // Convert letter codes to numeric: h=80, n=60, l=40
                        fire[header] = value === 'h' ? 80 : value === 'n' ? 60 : value === 'l' ? 40 : 0;
                    }
                } else {
                    fire[header] = value;
                }
            });
            
            // Add satellite name from source
            fire.satellite = satelliteMap[source] || source;
            
            // Handle different field names between MODIS and VIIRS
            if (headers.includes('bright_ti4')) {
                fire.brightness = fire.bright_ti4 as number;
                fire.brightness_2 = fire.bright_ti5 as number;
            }
            
            if (!isNaN(fire.latitude as number) && !isNaN(fire.longitude as number)) {
                fires.push(fire as unknown as FireData);
            }
        }
        
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

        const sources = [
            'MODIS_NRT',
            'VIIRS_SNPP_NRT',
            'VIIRS_NOAA20_NRT',
            'VIIRS_NOAA21_NRT'
        ];
        
        // Convert BBOX from minLat,minLon,maxLat,maxLon to west,south,east,north
        const [minLat, minLon, maxLat, maxLon] = env.BBOX.split(',').map(Number);
        const areaCoords = `${minLon},${minLat},${maxLon},${maxLat}`;

        let allFires: FireData[] = [];

        // Fetch data from CSV API sources
        for (const source of sources) {
            try {
                const firmsUrl = `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${env.MAP_KEY}/${source}/${areaCoords}/1`;
                const firmsRes = await fetch(firmsUrl);
                
                if (!firmsRes.ok) {
                    console.warn(`FIRMS API returned ${firmsRes.status} for ${source}: ${firmsRes.statusText}`);
                    continue;
                }
                
                const csvText = await firmsRes.text();
                const fires = this.parseCSV(csvText, source);
                allFires = allFires.concat(fires);
                console.log(`Found ${fires.length} fire detections from ${source} API`);
            } catch (error) {
                console.error(`Error fetching ${source} API:`, error);
            }
        }
        
        // Fetch data from KML sources
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
                
                console.log(`KMZ entries for ${kmlSource.name}:`, entries.map(e => e.entryName));
                for (const entry of entries) {
                    if (entry.entryName.endsWith('.kml')) {
                        const kmlContent = entry.getData().toString('utf8');
                        console.log(`KML content length for ${kmlSource.name}:`, kmlContent.length);
                        const fires = this.parseKML(kmlContent, kmlSource.name);
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

        // Compute clusters across full batch (before filtering or ephemeral checks)
        const maskingEnabled = env.LAND_COVER_MASKING && !!env.LRIS_API_KEY;
        const clusterMap = new Map<FireData, ClusterResult>();
        if (maskingEnabled) {
            for (const fire of allFires) {
                clusterMap.set(fire, this.findCluster(fire, allFires));
            }
        }

        // Load ephemeral state
        const ephemeral = maskingEnabled ? await this.ephemeral(EphemeralSchema) : { assessments: {} };
        if (!ephemeral.assessments) ephemeral.assessments = {};

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
        let filteredCount = 0;

        // Process each fire detection
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

                // Land cover masking pipeline
                let assessment: Assessment | null = null;
                if (maskingEnabled) {
                    const cachedKey = `${fire.latitude.toFixed(3)}_${fire.longitude.toFixed(3)}_${fire.acq_date}_${timeStr}`;
                    const cached = ephemeral.assessments[cachedKey];

                    if (cached?.landCover) {
                        assessment = cached as Assessment;
                    } else {
                        const pixelSize = fire.satellite.includes('VIIRS') ? 375 : 1000;
                        const bbox = this.computeFootprintBbox(fire.latitude, fire.longitude, pixelSize);
                        const cluster = clusterMap.get(fire) || { corroborated: false, clusterSize: 1, satellites: [fire.satellite] };

                        // Query LRIS + FENZ in parallel
                        const [landCoverFeatures, fireSeason] = await Promise.all([
                            this.queryLandCover(bbox, env.LRIS_API_KEY!),
                            env.FIRE_SEASON_AWARE ? this.queryFireSeason(fire.longitude, fire.latitude) : Promise.resolve(null)
                        ]);

                        const classification = this.classifyDetection(landCoverFeatures, fire.frp, acqDateTimeUTC, env, cluster, fireSeason);

                        assessment = {
                            landCover: landCoverFeatures ? {
                                primaryClass: classification.classes.reduce((best, c) => RISK_PRIORITY[c.risk] > RISK_PRIORITY[best.risk] ? c : best, classification.classes[0])?.name || 'Unknown',
                                classId: classification.classes.reduce((best, c) => RISK_PRIORITY[c.risk] > RISK_PRIORITY[best.risk] ? c : best, classification.classes[0])?.classId || 0,
                                riskLevel: classification.riskLevel,
                                passThrough: classification.passThrough,
                                allClasses: classification.classes
                            } : null,
                            fireSeason,
                            cluster,
                            timestamp: new Date().toISOString()
                        };

                        ephemeral.assessments[cachedKey] = assessment;
                    }

                    // Apply filter decision
                    if (assessment.landCover && !assessment.landCover.passThrough) {
                        filteredCount++;
                        continue;
                    }
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

                if (assessment?.landCover) {
                    const lc = assessment.landCover;
                    const classNames = lc.allClasses.map(c => `${c.name} (Class ${c.classId})`).join(', ');
                    remarkLines.push(`Land Cover: ${classNames}`);
                    if (lc.allClasses.length > 1) {
                        remarkLines.push(`Land Cover Risk: ${lc.riskLevel} (highest of ${lc.allClasses.length} classes)`);
                    } else {
                        remarkLines.push(`Land Cover Risk: ${lc.riskLevel}`);
                    }
                    callsign = `FIRMS Detection - FRP: ${fire.frp} - ${lc.primaryClass} (${lc.riskLevel})`;
                }

                if (assessment?.cluster?.corroborated) {
                    remarkLines.push(`Corroborated: ${assessment.cluster.clusterSize} detections within 500m/2h (${assessment.cluster.satellites.join(', ')})`);
                    callsign += ` [${assessment.cluster.clusterSize}x]`;
                }

                if (assessment?.fireSeason) {
                    const fs = assessment.fireSeason;
                    remarkLines.push(`Fire Season: ${fs.season} (${fs.zone}, ${fs.district})`);
                    if (fs.onDocLand) remarkLines.push('DoC Conservation Land: Yes');
                    if (fs.section52FireLandMgmt !== null) {
                        remarkLines.push(`Section 52: Fire for land management — ${fs.section52FireLandMgmt ? 'allowed' : 'prohibited'}`);
                    }
                } else if (maskingEnabled && env.FIRE_SEASON_AWARE) {
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
                    version: fire.version
                };

                if (assessment?.landCover) {
                    metadata.land_cover = assessment.landCover.primaryClass;
                    metadata.land_cover_risk = assessment.landCover.riskLevel;
                }
                if (assessment?.cluster) {
                    metadata.cluster_size = assessment.cluster.clusterSize;
                }
                if (assessment?.fireSeason) {
                    metadata.fire_season = assessment.fireSeason.season;
                }

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

        // Prune ephemeral entries older than 25 hours and save
        if (maskingEnabled) {
            const cutoff = Date.now() - 25 * 3600000;
            for (const [key, entry] of Object.entries(ephemeral.assessments)) {
                if (new Date(entry.timestamp).getTime() < cutoff) {
                    delete ephemeral.assessments[key];
                }
            }
            await this.setEphemeral(ephemeral);
        }

        if (filteredCount > 0) console.log(`Land cover masking filtered ${filteredCount} detections`);
        console.log(`ok - obtained ${fc.features.length} FIRMS fire features`);
        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

