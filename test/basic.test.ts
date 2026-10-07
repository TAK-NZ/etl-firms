import test from 'node:test';
import assert from 'node:assert';
import { SchemaType, DataFlowType, InvocationType, StaticCapabilities } from '@tak-ps/etl';

// task.ts calls Task.init() at module scope which requires an ETL environment,
// so these must be set before the dynamic import below
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: Task } = await import('../task.js');

type SchemaProps = Record<string, { type?: string; default?: unknown }>;

test('Task static config', () => {
    assert.equal(Task.name, 'etl-firms');
    assert.deepEqual(Task.flow, [DataFlowType.Incoming]);
    assert.deepEqual(Task.invocation, [InvocationType.Schedule]);
});

test('Incoming Input schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    const props = schema.properties as SchemaProps;
    for (const key of [
        'MAP_KEY',
        'BBOX',
        'MIN_CONFIDENCE',
        'MIN_FRP',
        'SHOW_FOOTPRINT',
        'FIRE_SEASON_AWARE',
        'FIRE_SEASON_CACHE_HOURS'
    ]) {
        assert.ok(props[key], `Env schema missing property: ${key}`);
    }

    assert.equal(props.BBOX.type, 'string');
    assert.equal(props.BBOX.default, '-47.3,166.3,-34.4,178.6');
    assert.equal(props.MIN_CONFIDENCE.type, 'number');
    assert.equal(props.MIN_CONFIDENCE.default, 50);
    assert.equal(props.MIN_FRP.type, 'number');
    assert.equal(props.MIN_FRP.default, 20);
    assert.equal(props.SHOW_FOOTPRINT.type, 'boolean');
    assert.equal(props.SHOW_FOOTPRINT.default, false);
    assert.equal(props.FIRE_SEASON_AWARE.type, 'boolean');
    assert.equal(props.FIRE_SEASON_AWARE.default, false);
    assert.equal(props.FIRE_SEASON_CACHE_HOURS.type, 'number');
    assert.equal(props.FIRE_SEASON_CACHE_HOURS.default, 6);
});

test('Incoming Output schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Output, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    const props = schema.properties as SchemaProps;
    for (const key of [
        'satellite',
        'time_since_detection',
        'acq_date',
        'acq_time',
        'acq_datetime',
        'brightness',
        'confidence',
        'brightness_2',
        'frp',
        'daynight',
        'version',
        'latitude',
        'longitude',
        'cluster_size',
        'fire_season'
    ]) {
        assert.ok(props[key], `Output schema missing property: ${key}`);
    }

    assert.equal(props.satellite.type, 'string');
    assert.equal(props.confidence.type, 'number');
    assert.equal(props.frp.type, 'number');
});

test('Outgoing flow is not provided', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Outgoing);

    assert.equal(schema.type, 'object');
    assert.deepEqual(schema.properties, {});
});

test('capabilities.json is valid', async () => {
    const doc = await StaticCapabilities.read(new URL('../capabilities.json', import.meta.url));

    assert.ok(doc, 'capabilities.json failed schema validation');
    assert.ok(doc.permissions?.some((p) => p.resource === 'feature:*' && p.required));
    assert.equal(doc.compute?.memory, 1024);
});
