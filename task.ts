
import { Type, TSchema } from '@sinclair/typebox';
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
    })
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
            
            // Debug: log first few descriptions
            if (nzCount <= 2) {
                console.log(`${source} NZ Fire #${nzCount} description sample:`, description.substring(0, 300));
            }
            
            // Extract data from KML description with correct HTML patterns
            const detectionTimeMatch = description.match(/Detection Time.*?(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2})/);
            const sensorMatch = description.match(/<b>Sensor: <\/b>\s*([^<]+)/);
            const confidenceMatch = description.match(/<b>Confidence[^:]*: <\/b>\s*([^<]+)/);
            const frpMatch = description.match(/<b>FRP: <\/b>\s*([\d.]+)/);
            const brightnessMatch = description.match(/<b>Brightness: <\/b>\s*([\d.]+)/);
            const dayNightMatch = description.match(/<b>Day\/Night: <\/b>\s*([^<]+)/);
            const scanMatch = description.match(/<b>Scan: <\/b>\s*([\d.]+)/);
            const trackMatch = description.match(/<b>Track: <\/b>\s*([\d.]+)/);
            
            if (nzCount <= 2) {
                console.log(`${source} matches:`, {
                    time: detectionTimeMatch,
                    sensor: sensorMatch,
                    conf: confidenceMatch,
                    frp: frpMatch,
                    bright: brightnessMatch
                });
            }
            
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
                return Environment
            } else {
                return Type.Object({})
            }
        } else {
            return Type.Object({})
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
        
        // Deduplicate fires based on coordinates and time
        const uniqueFires = new Map<string, FireData>();
        for (const fire of allFires) {
            const key = `${fire.latitude.toFixed(5)}_${fire.longitude.toFixed(5)}_${fire.acq_date}_${fire.acq_time}`;
            if (!uniqueFires.has(key)) {
                uniqueFires.set(key, fire);
            }
        }
        allFires = Array.from(uniqueFires.values());

        console.log(`Found ${allFires.length} total fire detections after deduplication`);

        const fc = {
            type: 'FeatureCollection' as const,
            features: [] as Array<{
                id: string;
                type: 'Feature';
                properties: Record<string, unknown>;
                geometry: { type: 'Point'; coordinates: [number, number] };
            }>
        };

        // Process each fire detection
        for (const fire of allFires) {
            // Filter by minimum confidence and FRP
            if (fire.confidence < env.MIN_CONFIDENCE || fire.frp < env.MIN_FRP) {
                continue;
            }
            
            try {
                const fireId = `${fire.satellite}_${fire.acq_date}_${fire.acq_time}_${fire.latitude}_${fire.longitude}`;
                
                // Parse acquisition datetime properly
                const timeStr = String(fire.acq_time).padStart(4, '0');
                const hours = timeStr.slice(0, 2);
                const minutes = timeStr.slice(2, 4);
                const acqDateTimeUTC = `${fire.acq_date}T${hours}:${minutes}:00Z`;
                const acqTime = new Date(acqDateTimeUTC);
                
                // Ensure brightness_2 exists for display
                const brightness2 = fire.brightness_2 || fire.brightness;
                
                // Calculate time since detection
                const now = new Date();
                const hoursSince = (now.getTime() - acqTime.getTime()) / (1000 * 60 * 60);
                
                let timeSinceCategory: string;
                if (hoursSince < 1) {
                    timeSinceCategory = '< 1';
                } else if (hoursSince < 3) {
                    timeSinceCategory = '1-3';
                } else if (hoursSince < 6) {
                    timeSinceCategory = '3-6';
                } else if (hoursSince < 12) {
                    timeSinceCategory = '6-12';
                } else {
                    timeSinceCategory = '12-24';
                }
                
                const feature = {
                    id: fireId,
                    type: 'Feature' as const,
                    properties: {
                        callsign: `FIRMS Detection - FRP: ${fire.frp}`,
                        type: 'a-f-X-i',
                        time: acqTime.toISOString(),
                        start: acqTime.toISOString(),
                        icon: Task.FIRE_ICON,
                        metadata: {
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
                        },
                        remarks: [
                            `Satellite: ${fire.satellite}`,
                            `Time since detection: ${timeSinceCategory} hours`,
                            `Acquisition (UTC): ${acqTime.toISOString().replace('T', ' ').replace('Z', '')}`,
                            `Acquisition (NZT): ${acqTime.toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' })}`,
                            `Confidence: ${fire.confidence}%`,
                            `Brightness temperature: ${fire.brightness}`,
                            `Fire Radiative Power (FRP): ${fire.frp}`
                        ].join('\n'),
                        archived: false
                    },
                    geometry: {
                        type: 'Point' as const,
                        coordinates: [fire.longitude, fire.latitude] as [number, number]
                    }
                };

                fc.features.push(feature);
            } catch (error) {
                console.error(`Error processing fire detection:`, error);
            }
        }

        console.log(`ok - obtained ${fc.features.length} FIRMS fire features`);
        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

