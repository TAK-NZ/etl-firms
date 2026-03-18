# FIRMS Land Cover Masking — Implementation Plan

## Objective

Add a spatial filter to the FIRMS ETL that classifies fire detections by land cover type using the NZ Land Cover Database (LCDB v6.0). This allows the pipeline to differentiate between likely wildfires and agricultural/scrub burns, reducing false positives and prioritising critical alerts.

## API Validation

The LCDB v6.0 WFS API was tested against a real FIRMS detection footprint and confirmed working.

### Corrected API Details

The original technical brief referenced `data.linz.govt.nz` — this is **incorrect**. The LCDB is hosted by Manaaki Whenua / Landcare Research on the LRIS portal.

| Parameter | Original (Incorrect) | Corrected |
|---|---|---|
| Host | `data.linz.govt.nz` | `lris.scinfo.org.nz` |
| API Key | LINZ Data Service key | **Separate LRIS API key required** |
| Geometry field | `shape` | `GEOMETRY` |
| CRS handling | Implicit | Must specify `'EPSG:4326'` in BBOX filter and `srsName=EPSG:4326` |

### Working Query

```
GET https://lris.scinfo.org.nz/services;key={LRIS_API_KEY}/wfs
  ?service=WFS
  &version=2.0.0
  &request=GetFeature
  &typeNames=layer-123148
  &outputFormat=json
  &srsName=EPSG:4326
  &propertyName=Name_2023,Class_2023
  &cql_filter=BBOX(GEOMETRY,{lon_min},{lat_min},{lon_max},{lat_max},'EPSG:4326')
```

Using `propertyName=Name_2023,Class_2023` is critical — without it, the response includes full polygon geometries (thousands of coordinate pairs per feature). With it, the response is ~500 bytes.

### Test Result

For the sample fire at **-39.951, 176.675** (FRP: 40.56 MW), the API returned 4 intersecting features:

| Feature ID | Name_2023 | Class_2023 |
|---|---|---|
| 1603383 | Exotic Forest | 71 |
| 1229301 | River | 21 |
| 1623907 | High Producing Exotic Grassland | 40 |
| 1097887 | High Producing Exotic Grassland | 40 |

Applying the "highest risk wins" rule → **Critical (Exotic Forest)** → Immediate Alert. This is the correct outcome for a 40.56 FRP detection touching forestry.

---

## ETL Configuration

New environment variables to add to the existing `Environment` Type.Object in task.ts:

```typescript
LRIS_API_KEY: Type.Optional(Type.String({
    description: 'LRIS (Manaaki Whenua) API key for land cover lookups. Get one free at https://lris.scinfo.org.nz/. If not provided, land cover masking is disabled and all detections pass through unfiltered.'
})),
LAND_COVER_MASKING: Type.Boolean({
    default: false,
    description: 'Enable land cover masking using LCDB v6.0 to classify fire detections. Requires LRIS_API_KEY.'
}),
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
    description: 'Filter out detections on urban land cover (Built-up Area, Transport Infrastructure, Surface Mine or Dump). Requires LAND_COVER_MASKING to be enabled.'
})
```

### Ephemeral State Schema

The ETL persists land cover and fire season assessments across invocations using the CloudTAK ephemeral state API (`this.ephemeral()` / `this.setEphemeral()`). This avoids redundant API calls when FIRMS reports the same detection across multiple polling cycles (up to 24h).

```typescript
const EphemeralSchema = Type.Object({
    assessments: Type.Record(Type.String(), Type.Object({
        landCover: Type.Optional(Type.Object({
            primaryClass: Type.String(),
            classId: Type.Number(),
            riskLevel: Type.String(),
            passThrough: Type.Boolean()
        })),
        fireSeason: Type.Optional(Type.Object({
            season: Type.String(),
            zone: Type.String(),
            onDocLand: Type.Boolean()
        })),
        cluster: Type.Optional(Type.Object({
            corroborated: Type.Boolean(),
            clusterSize: Type.Number(),
            satellites: Type.Array(Type.String())
        })),
        timestamp: Type.String({ format: 'date-time' })
    }))
});
```

The record key is the fire detection ID (same dedup key used elsewhere: `lat_lon_date_time`). Entries older than 25 hours are pruned on each invocation.

### Fallback Behaviour

When `LRIS_API_KEY` is not provided or `LAND_COVER_MASKING` is `false`:
- All detections pass through using the existing `MIN_FRP` and `MIN_CONFIDENCE` filters only
- No WFS queries are made
- No land cover metadata is added to the CoT

When the LRIS API is unreachable or returns an error:
- **Fail-open**: the detection passes through unfiltered
- Log a warning with the error
- Add `Land Cover: Lookup failed` to remarks so operators know the classification is missing

---

## Land Cover Classification

### Complete LCDB v6.0 Class Mapping

All 33 mainland classes must be mapped. The `Name_2023` attribute is used for classification.

| Risk Level | LCDB Classes | Pipeline Action |
|---|---|---|
| **Critical** | Indigenous Forest, Exotic Forest | **Immediate Alert.** Bypass all intensity filters. |
| **High** | Broadleaved Indigenous Hardwoods, Deciduous Hardwoods | Pass through if `FRP > FRP_THRESHOLD_HIGH`. |
| **Medium** | Manuka and/or Kanuka, Matagouri or Grey Scrub, Sub Alpine Shrubland, Fernland, Flaxland | Pass through if `FRP > FRP_THRESHOLD_MEDIUM` OR detection is between 9:00 PM – 6:00 AM NZST/NZDT. |
| **Low** | High Producing Exotic Grassland, Low Producing Grassland, Tall Tussock Grassland, Short-rotation Cropland, Orchard Vineyard and Other Perennial Crops | **Likely farm burn.** Filter out if `FRP < FRP_THRESHOLD_LOW_GRASS` AND during daylight hours (6:00 AM – 9:00 PM NZST/NZDT). |
| **Low** | Gorse and/or Broom | **Likely scrub burn.** Filter out if `FRP < FRP_THRESHOLD_LOW_SCRUB`. |
| **Ignore** | Built-up Area (settlement), Urban Parkland/Open Space, Transport Infrastructure, Surface Mine or Dump, Not land | Filter out if `FILTER_URBAN_HEAT` is enabled — urban heat sources, not wildfire. When disabled, pass through with existing filters. |
| **Pass-through** | All other classes (Alpine Grass/Herbfield, Gravel or Rock, Permanent Snow and Ice, Lake or Pond, River, Estuarine Open Water, Herbaceous Freshwater Vegetation, Herbaceous Saline Vegetation, etc.) | Pass through using existing `MIN_FRP` filter — no special handling. |

### Night-time Override

Any detection between **9:00 PM – 6:00 AM NZST/NZDT** on Medium or Low risk land cover bypasses the FRP threshold filters. Farmers don't burn at night.

Time conversion must use `Pacific/Auckland` timezone to correctly handle daylight saving.

### Detection Clustering (Corroboration)

Multiple satellites may detect the same fire on consecutive passes. For example, VIIRS NOAA-20 and VIIRS NOAA-21 often pass within ~45 minutes of each other — producing two independent detections of the same event ~260m apart. A single borderline detection on scrubland might be a farm burn; two independent satellite detections at the same spot almost certainly isn't.

Before applying FRP threshold filters, scan the current detection batch for nearby detections:

- **Cluster radius**: 500m (roughly one MODIS pixel)
- **Time window**: 2 hours
- **Minimum cluster size**: 2 detections (the current detection + at least one neighbour)

A detection is **corroborated** if at least one other detection (after deduplication) exists within the cluster radius and time window. This covers all meaningful scenarios:

- **Same satellite, same pass** — multiple adjacent pixels lit up, indicating a larger fire (two farm burns 300m apart at the exact same moment is implausible)
- **Same satellite, different pass** — the fire persisted or moved, which is wildfire behaviour
- **Different satellite** — independent confirmation

The deduplication step already removes true duplicates (same coordinates, same time), so any remaining neighbour within the cluster is a genuinely distinct detection.

Corroboration effects:

| Risk Level | Normal | Corroborated |
|---|---|---|
| Critical | Immediate alert | Immediate alert (no change) |
| High | FRP > `FRP_THRESHOLD_HIGH` | FRP > `FRP_THRESHOLD_HIGH` (no change) |
| Medium | FRP > `FRP_THRESHOLD_MEDIUM` or night-time | **Bypass FRP threshold** — pass through |
| Low (Grassland) | FRP > `FRP_THRESHOLD_LOW_GRASS` or night-time | **Bypass FRP threshold** — pass through |
| Low (Scrub) | FRP > `FRP_THRESHOLD_LOW_SCRUB` | **Bypass FRP threshold** — pass through |
| Ignore (Urban) | Filter if `FILTER_URBAN_HEAT` | Filter if `FILTER_URBAN_HEAT` (no change) |

The logic is simple: if two satellites independently see it, it's real. This only overrides FRP thresholds for Medium/Low risk classes — it doesn't promote a detection to a higher risk level, and it doesn't override the urban heat filter.

Implementation: this is a spatial scan within the `allFires` array after deduplication but before filtering or ephemeral cache checks. No external API calls needed. Use a brute-force distance check — with typical NZ detection counts (<100), this is O(n²) but negligible.

Clustering must run on the full batch first because it determines whether FRP thresholds are bypassed. The corroboration status is computed once when a detection is first seen and then cached to ephemeral state. If a later batch introduces a new neighbour that would corroborate a previously-assessed detection, the original assessment is **not** updated — but the new detection itself will be corroborated (it has a neighbour), so the operator still gets alerted to the fire.

```typescript
private findCluster(
    fire: FireData,
    allFires: FireData[],
    radiusMeters: number = 500,
    timeWindowHours: number = 2
): { corroborated: boolean; clusterSize: number; satellites: string[] }
```

### Multiple Feature Handling

A single fire footprint may intersect multiple land cover polygons. Use the **highest risk** class found:

```
Critical > High > Medium > Low > Ignore
```

If any feature is Critical, the detection is Critical regardless of other intersecting classes.

---

## CoT Metadata Enrichment

When land cover masking is enabled and a lookup succeeds, add the following to the feature:

### Remarks (human-readable)

Append to the existing remarks string:

```
Land Cover: Exotic Forest (Class 71)
Land Cover Risk: Critical
```

For multiple intersecting classes:

```
Land Cover: Exotic Forest (Class 71), High Producing Exotic Grassland (Class 40)
Land Cover Risk: Critical (highest of 2 classes)
```

When corroborated by multiple satellites:

```
Corroborated: 2 detections within 500m/2h (NOAA-20, NOAA-21)
```

### Metadata (machine-readable)

Add to the existing `metadata` object:

```typescript
metadata: {
    // ... existing fields ...
    land_cover: {
        primary_class: 'Exotic Forest',
        primary_class_id: 71,
        risk_level: 'Critical',
        all_classes: [
            { name: 'Exotic Forest', class_id: 71, risk: 'Critical' },
            { name: 'High Producing Exotic Grassland', class_id: 40, risk: 'Low' }
        ],
        lookup_time_ms: 245
    },
    cluster: {
        corroborated: true,
        cluster_size: 2,
        satellites: ['VIIRS NOAA-20', 'VIIRS NOAA-21']
    }
}
```

### Callsign Update

When land cover data is available, update the callsign to include the risk context:

```
Current:  "FIRMS Detection - FRP: 40.56"
Updated:  "FIRMS Detection - FRP: 40.56 - Exotic Forest (Critical)"
With cluster: "FIRMS Detection - FRP: 40.56 - Exotic Forest (Critical) [2x]"
```

---

## Implementation

### New Function: queryLandCover

```typescript
private async queryLandCover(
    bbox: { lonMin: number; latMin: number; lonMax: number; latMax: number },
    apiKey: string
): Promise<LandCoverResult | null>
```

- Constructs the WFS URL with `propertyName=Name_2023,Class_2023` to minimise response size
- Returns parsed features or `null` on failure (fail-open)
- Logs warnings on API errors

### New Function: classifyDetection

```typescript
private classifyDetection(
    landCoverFeatures: LandCoverFeature[],
    frp: number,
    acqTimeUTC: string,
    thresholds: FRPThresholds,
    corroboration: { corroborated: boolean; clusterSize: number; satellites: string[] },
    fireSeason: { season: string; zone: string; onDocLand: boolean } | null
): { passThrough: boolean; riskLevel: string; classes: LandCoverFeature[] }
```

This is the single classification function that takes all inputs and returns the final pass/filter decision. The evaluation order within the function:

1. Determine the highest risk level from land cover features
2. If Critical → pass through immediately (no further checks)
3. If Ignore and `FILTER_URBAN_HEAT` → filter out immediately
4. Check overrides that bypass FRP thresholds:
   - Night-time (9 PM – 6 AM NZST/NZDT) → bypass for Medium/Low
   - Corroborated (different satellite within 500m/2h) → bypass for Medium/Low
   - Restricted/Prohibited fire season → bypass for Medium/Low
5. If no override, apply FRP threshold for the risk level, with 1.5× multiplier during Open fire season for Medium/Low
6. Return the decision and all metadata for enrichment

### Integration Point — Processing Order

The full processing pipeline in `control()`:

```
Fetch all sources (CSV + KML)
  → Deduplicate (existing logic)
  → Compute clusters across full batch (findCluster for each fire)
  → Load ephemeral state
  → For each fire:
      1. Apply existing MIN_CONFIDENCE / MIN_FRP filters
      2. If masking disabled → add feature as-is (existing behaviour)
      3. If masking enabled:
         a. Check ephemeral cache by detection ID
         b. If cache hit → use cached assessment
         c. If cache miss:
            - Calculate footprint BBOX (375m VIIRS / 1km MODIS) — always, regardless of SHOW_FOOTPRINT
            - Query LRIS land cover + FENZ fire season in parallel (Promise.all)
            - Call classifyDetection() with land cover, fire season, and corroboration
            - Cache assessment to ephemeral state
         d. If passThrough is false → skip feature
         e. If passThrough is true → enrich feature with metadata/remarks
  → Prune ephemeral entries older than 25 hours
  → Save ephemeral state
  → Submit feature collection
```

Key ordering constraint: clustering must happen before the per-fire loop because it scans the full batch. Ephemeral cache is checked inside the loop because it's keyed per detection.

### Performance Considerations

- Each detection triggers one WFS query (~200-500ms based on testing)
- For burst events (50+ detections), this adds 10-25 seconds of sequential latency
- **Mitigation**: Two-tier caching strategy:
  1. **In-memory cache** (keyed by rounded BBOX coordinates, 0.01° grid) — avoids duplicate queries for clustered detections within the same Lambda invocation
  2. **Ephemeral state** — persist land cover and fire season assessments across invocations via `this.ephemeral()` / `this.setEphemeral()` (see [etl-inreach](https://github.com/TAK-NZ/etl-inreach) for the pattern). FIRMS reports the same detection for up to 24 hours, polled every 20–30 seconds — caching the assessment on first lookup avoids redundant API calls for subsequent polls of the same fire
- The `propertyName` filter keeps response payloads small (~500 bytes vs ~50KB+ with geometries)

---

## Testing

### Manual Verification

Test with known locations:

| Location | Expected Land Cover | Expected Risk |
|---|---|---|
| -39.951, 176.675 | Exotic Forest + Grassland | Critical |
| -41.29, 174.78 | Built-up Area (Wellington CBD) | Ignore |
| -43.53, 172.63 | High Producing Exotic Grassland (Canterbury Plains) | Low |
| -44.05, 168.66 | Indigenous Forest (Fiordland) | Critical |

### Edge Cases

- Detection on a river/lake boundary (should pass-through with default filters)
- Detection exactly on a forest/grassland boundary (should resolve to Critical)
- LRIS API timeout (should fail-open and log warning)
- Detection outside NZ mainland (LCDB has no coverage — should pass-through)
- Two detections from different satellites ~260m apart, 47 min apart on forest/grassland boundary (should both be Critical, both corroborated)
- Single low-FRP detection on scrubland (should be filtered) vs two low-FRP detections on scrubland from different satellites (should pass through as corroborated)

---

## FENZ Fire Season API — Check It's Alright

### Overview

Fire and Emergency New Zealand (FENZ) operates [checkitsalright.nz](https://www.checkitsalright.nz/) to inform the public whether outdoor fires are permitted at a given location and date. The site is a JavaScript SPA, but its backend uses **public ArcGIS REST APIs with no authentication required**. These APIs can be queried directly to determine the fire season status for any FIRMS detection.

This is valuable because during an **Open** fire season, low-FRP detections on farmland are far more likely to be permitted land management burns. During a **Restricted** or **Prohibited** season, the same detection is more suspicious and should be treated with higher priority.

### API Endpoints

The site queries three ArcGIS layers plus a NIWA weather API. All are unauthenticated.

| Layer | URL | Purpose |
|---|---|---|
| Fire Season | `https://utility.arcgis.com/usrsvcs/servers/b381ba9bdd4046c2b71a6b1a1e39da78/rest/services/FENZ/MapServer/27` | Returns the current fire season status (Open/Restricted/Prohibited) for a location |
| DoC Public Conservation Land | `https://utility.arcgis.com/usrsvcs/servers/e6865ced067f4234b047732b4d38a711/rest/services/FENZ_PCL/FeatureServer/0` | Determines if a location is on DoC land (upgrades Open → Restricted) |
| Section 52 Localities | `https://services1.arcgis.com/dcVyucRsXCQAi3GL/arcgis/rest/services/Section_52_Localities_v2_view/FeatureServer/0` | Activity-specific prohibitions under Section 52 of the FENZ Act |
| Nearest Weather Station | `https://www.checkitsalright.nz/api/v1/station/{longitude}/{latitude}` | Returns the nearest NIWA fire weather station |
| Fire Danger Index | `https://api.niwa.co.nz/fireweather/site/{stationId}/danger?apikey={key}` | Current fire danger rating (Low/Moderate/High/Very High/Extreme) |

### Working Query — Fire Season Status

```
GET https://utility.arcgis.com/usrsvcs/servers/b381ba9bdd4046c2b71a6b1a1e39da78/rest/services/FENZ/MapServer/27/query
  ?f=json
  &geometry={longitude},{latitude}
  &geometryType=esriGeometryPoint
  &inSR=4326
  &spatialRel=esriSpatialRelIntersects
  &outFields=Name,SEASON,Region,District,Zone_
  &returnGeometry=false
```

**Critical**: The `inSR=4326` parameter is required — without it, the query returns zero features.

### Test Results

| Location | Coordinates | SEASON | Zone | District |
|---|---|---|---|---|
| Hawke's Bay (sample fire) | 176.67471, -39.95065 | **Open** | Tukituki East | Hawke's Bay |
| Wellington CBD | 174.77, -41.29 | **Open** | Wellington | Wellington |
| Cape Reinga (Northland) | 172.68, -34.42 | **Prohibited** | Muri Whenua | Northland |

### Season Status Values

The `SEASON` field returns one of:

| Status | Meaning | Implication for FIRMS |
|---|---|---|
| `Open` | No restrictions on outdoor fires | Low-FRP detections on farmland are very likely permitted burns → lower priority |
| `Restricted` | Fire permit required for outdoor fires | Unpermitted burns less likely → moderate priority |
| `Prohibited` | All outdoor fires banned | Any detection is highly suspicious → higher priority |
| `Defence` | Civil defence emergency | Mapped to Prohibited |
| `Prohibition in open air` | Open-air fires banned | Mapped to Prohibited |

### DoC Land Upgrade Rule

If a location is on DoC Public Conservation Land and the fire season is `Open`, the effective status is upgraded to `Restricted`. This matches the FENZ website logic — fires on public conservation land always require a permit.

### Section 52 — Activity-Specific Prohibitions

The Section 52 layer contains per-activity prohibition flags. For land management fires, the relevant field is `Fire_Land_Management`:
- Value `1` = activity is allowed (subject to fire season)
- Value `0` = activity is specifically prohibited under Section 52

Test result for Hawke's Bay (176.67471, -39.95065):
```json
{
  "Fire_Land_Management": 1,
  "Fireworks": 0,
  "Comment": "All private use of fireworks and sky lanterns in the following areas in Hawke's Bay are prohibited from 19 December – 31 March"
}
```

In this case, land management fires are allowed but fireworks are prohibited — the Section 52 restriction is activity-specific.

### Proposed Integration with Land Cover Masking

The fire season status can be combined with the LCDB land cover classification to further reduce false positives:

| Fire Season | Land Cover Risk | Action |
|---|---|---|
| Open | Low (Grassland) | **Very likely farm burn.** Apply 1.5× FRP threshold (e.g. 40 → 60 MW) |
| Open | Low (Scrub) | **Likely controlled burn.** Apply 1.5× FRP threshold (e.g. 30 → 45 MW) |
| Open | Medium | Likely controlled burn. Apply 1.5× FRP threshold (e.g. 25 → 37.5 MW) |
| Restricted/Prohibited | Low (Grassland/Scrub) | Suspicious — no permit expected. **Bypass FRP threshold** — pass through |
| Restricted/Prohibited | Medium | Suspicious. **Bypass FRP threshold** — pass through |
| Restricted/Prohibited | Critical (Forest) | **Immediate alert.** No change from current behaviour |
| Any | Critical/High | No fire season adjustment — existing rules apply |
| Any | Ignore (Urban) | Filter if `FILTER_URBAN_HEAT` — unchanged |

The 1.5× multiplier is applied to the configured FRP thresholds at runtime — no new environment variables needed. The logic: during Open season, farmers are legally burning, so we need a higher bar to distinguish wildfires from permitted burns. During Restricted/Prohibited, any fire on low-risk land is suspicious and should pass through regardless of FRP.

Note: the night-time override and corroboration override still apply on top of fire season adjustments. If a detection is at night or corroborated, it bypasses FRP thresholds regardless of fire season.

### ETL Configuration

New environment variables:

```typescript
FIRE_SEASON_AWARE: Type.Boolean({
    default: false,
    description: 'Enable FENZ fire season lookups to adjust detection priority. Uses public ArcGIS APIs (no key required). If enabled, detections during Open season on low-risk land cover are deprioritised.'
})
```

No API key is needed — all FENZ ArcGIS endpoints are public.

### Fallback Behaviour

When the FENZ ArcGIS API is unreachable:
- **Fail-open**: treat as if fire season is unknown
- Log a warning
- Add `Fire Season: Lookup failed` to remarks
- Apply land cover classification without fire season adjustment

### Performance

- The fire season query is a simple point-in-polygon against ArcGIS (~100-300ms)
- Can be parallelised with the LRIS land cover query since they are independent
- Fire season zones are large — the same in-memory cache (0.1° grid) used for land cover will effectively cache fire season results for all detections in the same zone
- Both land cover and fire season results should be persisted to ephemeral state (keyed by fire detection ID) so repeat polls of the same detection skip all external API calls

### GeoJSON / KMZ Map Overlay

The fire season layer supports multiple output formats for map overlays:

| Format | URL Parameter | Use Case |
|---|---|---|
| GeoJSON | `f=geojson` | Web maps, programmatic processing |
| KMZ (native) | `f=kmz` | Google Earth, TAK — but uses generic styling |
| JSON | `f=json` | ArcGIS clients |

The full dataset is 227 zones. Response sizes with geometry simplification (`maxAllowableOffset`):

| Simplification | Approx. Accuracy | GeoJSON Size |
|---|---|---|
| None | Full resolution | ~25 MB |
| `0.001` (~100m) | Good for regional view | ~1.4 MB |
| `0.005` (~500m) | Fine for national view | ~440 KB |

Current zone breakdown (as of testing): 165 Open, 29 Restricted, 25 Prohibited, 8 Defence.

**Full GeoJSON query:**
```
GET https://utility.arcgis.com/usrsvcs/servers/b381ba9bdd4046c2b71a6b1a1e39da78/rest/services/FENZ/MapServer/27/query
  ?f=geojson
  &where=1=1
  &outFields=Name,SEASON,Region,District,Zone_
  &returnGeometry=true
  &outSR=4326
  &resultRecordCount=227
  &maxAllowableOffset=0.001
```

#### Colour-Coded KMZ for TAK

The native `f=kmz` output uses a single generic style — it does **not** colour-code zones by season status. To produce a colour-coded KMZ overlay for TAK/ATAK:

1. Fetch the GeoJSON (simplified to ~1.4 MB)
2. Generate KML with `<Style>` elements per season status:
   - **Open** → Green (`#ff00ff00`, 30% fill opacity)
   - **Restricted** → Orange (`#ffff8800`, 40% fill opacity)
   - **Prohibited** → Red (`#ffff0000`, 50% fill opacity)
   - **Defence** → Red (same as Prohibited)
3. Compress as KMZ

This could be implemented as:
- A standalone script that generates a static KMZ on a schedule
- A small Lambda behind a URL that serves a `<NetworkLink>` KML, so TAK clients auto-refresh when FENZ updates zone statuses
- A separate ETL task in CloudTAK that pushes fire season zones as CoT polygons

This is **out of scope** for the FIRMS ETL but documented here as a potential companion overlay.

---

## Questions & Open Items

1. ~~**LRIS API key provisioning** — Who manages the LRIS API key?~~ **Resolved** — Out of scope for the ETL. The admin is responsible for obtaining an LRIS API key from https://lris.scinfo.org.nz/ (free, requires access to layer-123148 LCDB v6.0). The ETL documentation and `LRIS_API_KEY` environment variable description will direct the admin to do this, same as with `MAP_KEY` for NASA FIRMS.

2. ~~**Rate limiting** — The LRIS WFS API rate limits are not documented.~~ **Resolved** — Two-tier caching: in-memory cache for within-invocation deduplication, plus ephemeral state persistence across invocations via `this.ephemeral()` / `this.setEphemeral()`. Since FIRMS reports the same detection for up to 24h (polled every 20–30s), the assessment is cached on first lookup and reused for all subsequent polls of the same fire.

3. ~~**Fire season integration** — This plan intentionally excludes the fire season API as a dependency.~~ **Resolved** — The FENZ fire season API has been validated and documented above. The `FIRE_SEASON_AWARE` toggle is now part of the plan.

4. ~~**Footprint vs centroid** — Should we always use the footprint BBOX for land cover queries, or only when `SHOW_FOOTPRINT` is on?~~ **Resolved** — Always calculate and use the footprint BBOX (375m for VIIRS, 1km for MODIS) for land cover and fire season queries. The `SHOW_FOOTPRINT` setting only controls whether the polygon is visualised on the map — the BBOX is always computed internally for spatial lookups.

5. ~~**Urban heat filtering** — Should this be a separate toggle?~~ **Resolved** — Yes, use a separate `FILTER_URBAN_HEAT` toggle so operators can choose whether to filter out Built-up Area, Transport Infrastructure, etc. Default: `false` (show all detections).

6. ~~**FENZ API stability** — Should we add a health check / version detection, or is fail-open sufficient?~~ **Resolved** — Fail-open is sufficient. Any FENZ API failures must be logged in the ETL log details so they can be investigated. If Alerts are enabled for the ETL task in CloudTAK, API failures will trigger alert notifications.
