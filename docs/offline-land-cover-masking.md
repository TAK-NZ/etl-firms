# Offline Land Cover Masking via S3-Hosted FlatGeobuf

## Background

The original `etl-firms` implementation queried the [LRIS WFS API](https://lris.scinfo.org.nz/) on every fire
detection to look up land cover class from the LCDB v6.0 dataset. This caused excessive load on the
Koordinates-hosted LRIS infrastructure and has been removed.

This document describes how to restore land cover masking using a pre-processed, S3-hosted copy of
the LCDB v6.0 dataset, queried entirely in-process with no external API calls at runtime.

---

## Architecture Overview

```
[One-time pre-processing]
  LCDB v6.0 GeoPackage (from LRIS)
    → ogr2ogr (simplify + attribute filter)
    → lcdb-v6.fgb  (FlatGeobuf, ~15–30 MB)
    → upload to S3 bucket

[ETL runtime — runs every 20 seconds]
  Cold start / first invocation
    → download lcdb-v6.fgb from S3  (~1–2 s, once per container)
    → deserialise with flatgeobuf npm package
    → build flatbush R-tree index in module-level singleton

  Each subsequent invocation
    → reuses in-memory index (no download, no I/O)
    → per-fire: flatbush bbox search → point-in-polygon ray-cast → land cover class
```

**Key properties:**
- Zero calls to lris.scinfo.org.nz or any Koordinates infrastructure at runtime
- One S3 download per container lifetime (not per invocation)
- Graceful degradation: if the S3 file is missing or corrupted, masking is skipped and all
  detections pass through unfiltered — no crash, no retry storm
- LCDB is static data (updated infrequently); the S3 file only needs refreshing when a new LCDB
  version is published

---

## Step 1 — Download LCDB v6.0

Download the GeoPackage from the LRIS portal (free account required, CC BY 4.0 licence):

```
https://lris.scinfo.org.nz/layer/123148-lcdb-v60-land-cover-database-version-60-mainland-new-zealand/
```

The downloaded archive will contain a file named something like
`lcdb-v60-land-cover-database-version-60-mainland-new-zealand.gpkg`.

---

## Step 2 — Pre-process to FlatGeobuf

Run the following `ogr2ogr` command (requires GDAL ≥ 3.1):

```bash
ogr2ogr \
  -f FlatGeobuf \
  -select Name_2023,Class_2023 \
  -t_srs EPSG:4326 \
  -simplify 0.0001 \
  lcdb-v6.fgb \
  "lcdb-v60-land-cover-database-version-60-mainland-new-zealand.gpkg"
```

**Flag rationale:**

| Flag | Purpose |
|------|---------|
| `-f FlatGeobuf` | Compact binary format with built-in spatial index; deserialises ~5× faster than JSON |
| `-select Name_2023,Class_2023` | Drops all attributes except the two the ETL needs, reducing file size |
| `-t_srs EPSG:4326` | Ensures coordinates are WGS84 lon/lat to match fire detection coordinates |
| `-simplify 0.0001` | Simplifies geometries to ~10 m tolerance — well within the 375 m VIIRS / 1 km MODIS pixel size, significantly reduces file size |

Expected output size: **15–30 MB**.

Verify the output:

```bash
ogrinfo -al -so lcdb-v6.fgb
# Should report ~350,000+ features, fields Name_2023 (String) and Class_2023 (Integer)
```

---

## Step 3 — Upload to S3

```bash
aws s3 cp lcdb-v6.fgb s3://<your-bucket>/lcdb-v6.fgb
```

The ETL will reference this file via the `LCDB_S3_URL` environment variable (see Step 5).

---

## Step 4 — Add npm Dependencies

```bash
npm install flatgeobuf flatbush
npm install --save-dev @types/flatbush
```

| Package | Purpose |
|---------|---------|
| `flatgeobuf` | Deserialises the FlatGeobuf binary format into GeoJSON features |
| `flatbush` | Pure-JS R-tree spatial index for fast bounding-box candidate queries |

No native addons — both packages work in Lambda and containerised environments.

---

## Step 5 — Environment Variable

Add `LCDB_S3_URL` to the `Environment` TypeBox schema in `task.ts`:

```typescript
LCDB_S3_URL: Type.Optional(Type.String({
    description: 'S3 URL (s3://bucket/key) of the pre-processed LCDB v6.0 FlatGeobuf file. ' +
                 'When absent, land cover masking is disabled and all detections pass through.'
})),
LAND_COVER_MASKING: Type.Boolean({
    default: false,
    description: 'Enable land cover masking using the offline LCDB v6.0 file at LCDB_S3_URL.'
}),
```

The remaining land cover environment variables from the original implementation should also be
restored alongside this — see the git history for the full set (`FRP_THRESHOLD_HIGH`,
`FRP_THRESHOLD_MEDIUM`, `FRP_THRESHOLD_LOW_SCRUB`, `FRP_THRESHOLD_LOW_GRASS`,
`FRP_CORROBORATED_LOW`, `FILTER_URBAN_HEAT`).

---

## Step 6 — Module-Level Index Singleton

Declare the cache **outside** the `Task` class at module scope so it survives across invocations
within the same container:

```typescript
import Flatbush from 'flatbush';
import { deserialize } from 'flatgeobuf';

interface LcdbFeature {
    name: string;
    classId: number;
    rings: number[][][];  // outer ring + holes, lon/lat pairs
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
}

interface LcdbIndex {
    tree: Flatbush;
    features: LcdbFeature[];
}

// undefined  = never attempted (will try on next invocation)
// null       = attempted but failed (skip masking, don't retry until container restarts)
// LcdbIndex  = loaded successfully
let lcdbIndex: LcdbIndex | null | undefined = undefined;
```

---

## Step 7 — Load Function

Add a standalone `loadLcdb` function (not a class method — it populates the module-level
singleton):

```typescript
async function loadLcdb(s3Url: string): Promise<void> {
    // Parse s3://bucket/key
    const withoutScheme = s3Url.replace(/^s3:\/\//, '');
    const slashIndex = withoutScheme.indexOf('/');
    const bucket = withoutScheme.slice(0, slashIndex);
    const key = withoutScheme.slice(slashIndex + 1);

    // Download from S3 using the AWS SDK (already available in Lambda/ECS environments)
    // or via a signed HTTPS URL stored in LCDB_S3_URL instead of s3:// URI.
    const res = await fetch(`https://${bucket}.s3.amazonaws.com/${key}`);
    if (!res.ok) throw new Error(`S3 fetch failed: ${res.status} ${res.statusText}`);

    const buffer = await res.arrayBuffer();

    // Deserialise FlatGeobuf → GeoJSON feature stream
    const features: LcdbFeature[] = [];
    for await (const f of deserialize(new Uint8Array(buffer))) {
        const geom = f.geometry;
        if (!geom || geom.type !== 'Polygon' && geom.type !== 'MultiPolygon') continue;

        const name = String(f.properties?.Name_2023 ?? 'Unknown');
        const classId = Number(f.properties?.Class_2023 ?? 0);

        // Flatten MultiPolygon into individual polygon rings sets
        const ringsets: number[][][][] =
            geom.type === 'MultiPolygon'
                ? (geom.coordinates as number[][][][])
                : [geom.coordinates as number[][][]];

        for (const rings of ringsets) {
            const outer = rings[0];
            let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            for (const [x, y] of outer) {
                if (x < minX) minX = x;
                if (y < minY) minY = y;
                if (x > maxX) maxX = x;
                if (y > maxY) maxY = y;
            }
            features.push({ name, classId, rings, minX, minY, maxX, maxY });
        }
    }

    // Build flatbush R-tree
    const tree = new Flatbush(features.length);
    for (const f of features) tree.add(f.minX, f.minY, f.maxX, f.maxY);
    tree.finish();

    lcdbIndex = { tree, features };
    console.log(`LCDB index loaded: ${features.length} polygons`);
}
```

---

## Step 8 — Per-Detection Query

Replace the original `queryLandCover` WFS call with a local index lookup. Add this as a class
method on `Task`:

```typescript
private queryLandCoverLocal(lon: number, lat: number): LandCoverFeature[] {
    if (!lcdbIndex) return [];

    // Candidate polygons whose bbox contains the fire centroid
    const candidates = lcdbIndex.tree.search(lon, lat, lon, lat);

    const matches: LandCoverFeature[] = [];
    for (const idx of candidates) {
        const feat = lcdbIndex.features[idx];
        if (pointInPolygon(lon, lat, feat.rings)) {
            matches.push({
                name: feat.name,
                classId: feat.classId,
                risk: LAND_COVER_RISK[feat.name] ?? 'Pass-through'
            });
        }
    }
    return matches;
}
```

Where `pointInPolygon` is a ray-casting helper (already existed in the original codebase as part
of `polygonIntersectsRect`):

```typescript
function pointInPolygon(x: number, y: number, rings: number[][][]): boolean {
    // Test outer ring; subtract holes
    for (let r = 0; r < rings.length; r++) {
        const ring = rings[r];
        let inside = false;
        for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
            const [xi, yi] = ring[i];
            const [xj, yj] = ring[j];
            if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) {
                inside = !inside;
            }
        }
        // Outer ring (r=0): must be inside. Holes (r>0): must NOT be inside.
        if (r === 0 && !inside) return false;
        if (r > 0 && inside) return false;
    }
    return true;
}
```

> **Note:** The original implementation used a bounding-box polygon intersection check (fire
> pixel footprint vs land cover polygon). This simplification to a centroid point-in-polygon check
> is appropriate because the VIIRS pixel (375 m) and MODIS pixel (1 km) are much larger than the
> ~10 m geometry simplification tolerance, and fire detection centroids are representative of the
> pixel location.

---

## Step 9 — Wire Into `control()`

At the top of `control()`, trigger the one-time load:

```typescript
async control(): Promise<void> {
    const env = await this.env(Environment);
    const maskingEnabled = env.LAND_COVER_MASKING && !!env.LCDB_S3_URL;

    // Load LCDB index on first invocation (module-level singleton)
    if (maskingEnabled && lcdbIndex === undefined) {
        try {
            await loadLcdb(env.LCDB_S3_URL!);
        } catch (err) {
            console.warn('Failed to load LCDB from S3, land cover masking disabled:', err);
            lcdbIndex = null;  // don't retry until container restarts
        }
    }

    // ... rest of control() unchanged ...
```

Then in the per-fire processing loop, replace the old `queryLandCover` call:

```typescript
// OLD (removed — caused load on LRIS/Koordinates):
// const landCoverFeatures = await this.queryLandCover(bbox, env.LRIS_API_KEY!);

// NEW — local index lookup, no external API call:
const landCoverFeatures = maskingEnabled && lcdbIndex
    ? this.queryLandCoverLocal(fire.longitude, fire.latitude)
    : null;
```

The rest of `classifyDetection` and the downstream filtering/remarks logic can be restored from
git history unchanged.

---

## Graceful Failure Behaviour

| Condition | Behaviour |
|-----------|-----------|
| `LAND_COVER_MASKING` is `false` | Masking skipped; all detections pass through |
| `LCDB_S3_URL` not set | Masking skipped; all detections pass through |
| S3 file does not exist (404) | Warning logged once; `lcdbIndex = null`; all detections pass through for container lifetime |
| S3 file is corrupt / parse error | Warning logged once; `lcdbIndex = null`; all detections pass through for container lifetime |
| S3 file loaded successfully | Normal masking behaviour |

The `null` sentinel ensures a broken S3 file does not cause a retry storm — the ETL will not
attempt to re-download until the container restarts (e.g. after a deployment that fixes the file).

---

## Keeping the Dataset Current

LCDB is updated infrequently (major versions every few years). When a new version is published:

1. Download the new GeoPackage from LRIS
2. Re-run the `ogr2ogr` pre-processing command from Step 2
3. Upload to S3, overwriting `lcdb-v6.fgb` (or version the key, e.g. `lcdb-v7.fgb`, and update
   the `LCDB_S3_URL` environment variable)
4. Restart or redeploy the ETL containers so they pick up the new file on next cold start

---

## Attribution

The LCDB v6.0 dataset is produced by Manaaki Whenua — Landcare Research and distributed under the
[Creative Commons Attribution 4.0 International licence](https://creativecommons.org/licenses/by/4.0/).
Attribution must be retained wherever the data is used or redistributed.

> Land Cover Database v6.0, Manaaki Whenua — Landcare Research, available from
> https://lris.scinfo.org.nz/layer/123148/
