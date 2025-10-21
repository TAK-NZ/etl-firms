
import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType, InputFeatureCollection } from '@tak-ps/etl';

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

    private parseCSV(csvText: string, typename: string): FireData[] {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return [];
        
        const headers = lines[0].split(',');
        const fires: FireData[] = [];
        
        // Map typename to satellite name
        const satelliteMap: Record<string, string> = {
            'ms:fires_modis_24hrs': 'MODIS',
            'ms:fires_snpp_24hrs': 'VIIRS S-NPP',
            'ms:fires_noaa20_24hrs': 'VIIRS NOAA-20',
            'ms:fires_noaa21_24hrs': 'VIIRS NOAA-21'
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
            
            // Add satellite name from typename
            fire.satellite = satelliteMap[typename] || typename;
            
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

        const typenames = [
            'ms:fires_modis_24hrs',
            'ms:fires_snpp_24hrs', 
            'ms:fires_noaa20_24hrs',
            'ms:fires_noaa21_24hrs'
        ];

        let allFires: FireData[] = [];

        // Fetch data from all satellite sources
        for (const typename of typenames) {
            try {
                const firmsUrl = new URL(`https://firms.modaps.eosdis.nasa.gov/mapserver/wfs/Australia_NewZealand/${env.MAP_KEY}/?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAME=${typename}&STARTINDEX=0&COUNT=1000&SRSNAME=urn:ogc:def:crs:EPSG::4326&outputformat=csv`);
                firmsUrl.searchParams.set('BBOX', `${env.BBOX},urn:ogc:def:crs:EPSG::4326`);

                const firmsRes = await fetch(firmsUrl);
                
                if (!firmsRes.ok) {
                    console.warn(`FIRMS API returned ${firmsRes.status} for ${typename}: ${firmsRes.statusText}`);
                    continue;
                }
                
                const csvText = await firmsRes.text();
                const fires = this.parseCSV(csvText, typename);
                allFires = allFires.concat(fires);
                console.log(`Found ${fires.length} fire detections from ${typename}`);
            } catch (error) {
                console.error(`Error fetching ${typename}:`, error);
            }
        }

        console.log(`Found ${allFires.length} total fire detections`);

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
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
                            brightness_2: fire.brightness_2,
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
                        coordinates: [fire.longitude, fire.latitude]
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

