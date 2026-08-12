# K1 — "group by EPSG → reproject → process → concat"

Investigate-only. This is a proposal, not an applied change.

## What was checked

The audit flagged six sites. There are in fact **seven loops** — `impact_polygons_pop.py`
contains two. None of them has been partly consolidated already; each was written
independently and they have drifted along five separate axes.

| # | Site | Zone column | How the zone list is built | Execution | Second frame | What it returns |
|---|---|---|---|---|---|---|
| 1 | `data_merge/correct_locations_w_OSM.py:97` | `epsg` | union of both frames' zones | sequential | paired by the **same** `epsg` value | concat of frames, back to 4326 |
| 2 | `data_merge/final_data_merge.py:~105` (`find_meter_coordinates`) | `epsg` | `df['epsg'].unique()`, `NaN` skipped | sequential | none | same frame plus a `meter_geometry` **WKT column**; geometry stays 4326 |
| 3 | `pop_at_risk_river_calculations/find_diff_pop.py:86` (`find_differences`) | `epsg` | zones of the population frame only | ProcessPool **or** sequential, config-driven | watersheds selected by **basin-id membership**, not by zone | concat of frames, back to 4326 |
| 4 | `create_voronoi.py:834` (`intersect_with_polygons_parallelized`) | `utm` | union of both frames' zones | sequential (two backends: sindex / DuckDB) | paired by the same zone value | concat of frames + an untouched `nans` bucket, 4326 |
| 5 | `pop_at_risk_river_calculations/find_intersection_river.py:127` | `utm` | zones of the polygon frame only | ProcessPool | rivers selected by **sindex bbox** of the subset | concat of frames, back to 4326 |
| 6 | `pop_at_risk_river_calculations/impact_polygons_pop.py:85` | `utm` | `river_gdf.utm.unique()`, `NaN` skipped | sequential | none | a **dict** `{id: geometry}`, never concatenated |
| 7 | `pop_at_risk_river_calculations/impact_polygons_pop.py:493` | `utm` | `final_df['utm'].unique()` | ProcessPool (`parallel_dissolve`) | none | concat of dissolved frames |

Two further differences worth naming, because they are the ones that would break a
naive shared helper:

- **Site 4 does not reproject.** It only partitions; the actual `to_crs` happens
  inside `intersect_with_polygons_db`, which takes the zone from `df['utm'].mode()[0]`.
  A helper that yields already-reprojected subframes would silently double-project it.
- **Site 2 does not replace the geometry.** It writes a metre-space WKT into a second
  column and leaves the active geometry in 4326. It is a projection *reader*, not a
  projection *pipeline*.

## Proposal

Do **not** build a "group, reproject, process, concat" helper. The `process` and
`concat` halves are where all seven disagree — output shape (frame / dict / extra
column), second-frame pairing (same zone / basin membership / sindex bbox), and
execution model (sequential / ProcessPool / config-selected) are genuine per-site
decisions, and a helper carrying all of them as parameters would be harder to read
than the seven loops it replaced.

Consolidate only the part that is actually identical — the partition:

```python
# geo_utils.py
def iter_crs_groups(gdf, column='utm', reproject=True, skip_missing=True):
    """Yield ``(epsg, subframe)`` for each distinct projected CRS in ``column``.

    ``reproject=False`` yields the subframes in their original CRS, for callers
    that reproject further downstream (``create_voronoi.intersect_with_polygons_db``
    does its own ``to_crs``).
    ``skip_missing`` drops rows whose zone is NaN instead of raising.
    """
```

and, for the five sites that end the same way:

```python
# geo_utils.py
def concat_groups(parts, crs=4326):
    """Concatenate group results into one GeoDataFrame in ``crs``, dropping empties."""
```

That removes the repeated `sub = gdf[gdf[col] == zone].copy().to_crs(zone)` line and
the repeated `pd.concat(...)` + `gpd.GeoDataFrame(..., crs=4326)` tail — roughly the
half of each loop that is genuinely the same — while leaving each site's real logic
where a reader can see it.

## Sequencing

This is a behaviour-preserving refactor of code with no characterization tests, and
sites 3, 5 and 7 run under `ProcessPoolExecutor` (so a generator has to be consumed
before submission, not lazily inside the worker loop). It should follow the same rule
as the rest of batch K: write the characterization tests first, then migrate one site
at a time. Recommended order, easiest first: 7, 6, 1, 5, 3, 2, 4 — with **site 4 last**,
since it is the one that does not reproject and is also the most heavily used path.
