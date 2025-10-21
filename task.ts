
import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType, InputFeatureCollection } from '@tak-ps/etl';

const Environment = Type.Object({
    MAP_KEY: Type.String({
        description: 'NASA FIRMS Map Key from https://firms2.modaps.eosdis.nasa.gov/mapserver/wfs-info/'
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

    private parseCSV(csvText: string): FireData[] {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return [];
        
        const headers = lines[0].split(',');
        const fires: FireData[] = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',');
            if (values.length !== headers.length) continue;
            
            const fire: any = {};
            headers.forEach((header, index) => {
                const value = values[index]?.trim();
                if (['latitude', 'longitude', 'brightness', 'scan', 'track', 'confidence', 'brightness_2', 'frp'].includes(header)) {
                    fire[header] = parseFloat(value);
                } else {
                    fire[header] = value;
                }
            });
            
            if (!isNaN(fire.latitude) && !isNaN(fire.longitude)) {
                fires.push(fire as FireData);
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



        // Build FIRMS URL with MAP_KEY and BBOX
        const firmsUrl = new URL(`https://firms2.modaps.eosdis.nasa.gov/mapserver/wfs/Australia_NewZealand/${env.MAP_KEY}/?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAME=ms:fires_modis_24hrs&STARTINDEX=0&COUNT=1000&SRSNAME=urn:ogc:def:crs:EPSG::4326&outputformat=csv`);
        firmsUrl.searchParams.set('BBOX', `${env.BBOX},urn:ogc:def:crs:EPSG::4326`);

        // Fetch FIRMS data
        const firmsRes = await fetch(firmsUrl);
        
        if (!firmsRes.ok) {
            throw new Error(`FIRMS API returned ${firmsRes.status}: ${firmsRes.statusText}`);
        }
        
        const csvText = await firmsRes.text();
        
        // Parse CSV data
        const fires = this.parseCSV(csvText);
        console.log(`Found ${fires.length} fire detections`);

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        // Process each fire detection
        for (const fire of fires) {
            // Filter by minimum confidence and FRP
            if (fire.confidence < env.MIN_CONFIDENCE || fire.frp < env.MIN_FRP) {
                continue;
            }
            
            try {
                const fireId = `${fire.satellite}_${fire.acq_date}_${fire.acq_time}_${fire.latitude}_${fire.longitude}`;
                
                const feature = {
                    id: fireId,
                    type: 'Feature' as const,
                    properties: {
                        callsign: `FIRMS Detection - FRP: ${fire.frp}`,
                        type: 'a-f-X-i',
                        time: new Date(fire.acq_datetime).toISOString(),
                        start: new Date(fire.acq_datetime).toISOString(),
                        icon: Task.FIRE_ICON,
                        metadata: {
                            satellite: fire.satellite,
                            acq_date: fire.acq_date,
                            acq_time: fire.acq_time,
                            acq_datetime: fire.acq_datetime,
                            brightness: fire.brightness,
                            confidence: fire.confidence,
                            brightness_2: fire.brightness_2,
                            frp: fire.frp,
                            daynight: fire.daynight,
                            version: fire.version
                        },
                        remarks: [
                            `Acquisition (UTC): ${fire.acq_datetime}`,
                            `Acquisition (NZT): ${new Date(fire.acq_datetime).toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' })}`,
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

