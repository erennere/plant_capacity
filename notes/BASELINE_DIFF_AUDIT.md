# Baseline diff audit — current `src/` vs the frozen old-version tree

Compares the working tree against `src/old-version-DO-NOT-CHANGE-THIS-ONLY-TO-COMPARE/`,
which is read-only reference material. Nothing in this audit wrote to it, `chmod`ed it, or
imported it as a package; where a wrapper had to actually run, it ran from a scratch copy
(`init_log` does `rm -f` on `${PROJECT_ROOT}/logs`, so running in place would have edited it).

Reproduce with the four harnesses under [src/tests/harness/](../src/tests/harness/) — see the
`baseline-diff-audit` skill for the procedure.

## What the baseline is

Not a git snapshot: its `starter.py` and `create_voronoi.py` match no commit in the last 20, and it
predates `geo_utils.py`/`utils.py` entirely. It is an older lineage — ~50 000 changed lines across
100 files — so a line-level diff proves nothing. Everything below compares behavior surfaces.

| | baseline | current |
|---|---|---|
| `.py` / `.sh` | 99 / 32 | 96 / 32 |
| config sections / leaf keys | 37 / 541 | 37 / 552 |
| top-level definitions | 957 | 998 |
| `geo_utils.py`, `utils.py` | absent | present |

## Verdict

**No regressions survive.** One real break was found and fixed during the audit (row R1).
Everything else is either an approved change, a confirmed bug fix, or unchanged.

| layer | result |
|---|---|
| 1 — API / config / constants / wrapper surfaces | all deltas classified, none unexplained |
| 2 — wrapper argv equivalence | 19 wrappers differ, all 4 causes traced; no wrapper lost a module or a flag |
| 3 — resolved config | 1 changed value across 37 sections, a confirmed fix |
| 4 — differential tests | 14 cases, 0 needing attention |
| 5 — runnability | 41/41 entry points OK after R1; 30 failed / 614 passed / 13 skipped, unchanged |

### Re-run 2026-08-11 — suite now fully green

Layer 5's failure count drifted **30 → 37** after this audit, during the rounds that followed
it (doc-sync, refactors 5.3/5.6/5.7, the seven banked prerequisites, and a since-reverted
Dagster spike). Those writeups all called the 37 "pre-existing", which was true relative to
*their own* starting point but false relative to this audit — the drift was theirs.

All 37 have since been traced and fixed, and the suite is now **0 failed / 648 passed /
13 skipped**. Root causes, none of them production regressions:

- 6 — 25 `.sh` wrappers had lost their executable bit (`core.fileMode=false` hides this from
  git). Restored with `chmod +x`; the baseline tree is *not* the authority here, since all 32
  of its `.sh` files are non-executable as a copy artifact.
- ~9 — mocks still patching the `duckdb` module global after refactor 5.5 routed access
  through `utils.duckdb_connection`.
- ~8 — tests patching `module.sys.argv` positionally after the argparse migration; the modules
  no longer `import sys`. Now driven through real `sys.argv` with the named flags.
- ~9 — fixtures missing config keys that became required (`tolerance`, `wwtp_buffer`,
  `country_output_column`, `annotated_all_filepath`, `industrial_batch_size`, …).
- 3 — `decode_gen_text` fixtures still using a pre-`Decision:` format. The parser itself is
  **byte-identical to the baseline**, so these were inherited test staleness, not a change.
- 1 — a test asserting `country_limit=0` truncates to 3 of 4 countries. `0` means "no limit"
  in the current tree, and the baseline has no `country_limit` key at all (also no limit), so
  the expectation matched neither tree.
- 3 deleted as invalid: two asserted a fiona/parquet fallback in `load_industrial_areas` that
  exists in **neither** tree, and one asserted `initialize_voronoi_weights` calls
  `auto_weight_scale` — the baseline has that call commented out with `factor = 1.0`, so
  `factor = 1.0` *is* baseline parity. Replaced by a test pinning exactly that.

Re-verified at the same layers: resolved-config diff still shows only the one approved change
(B3, `eu_ref_filepath`); all 30 runnable wrappers exit 0 with a correct `-m module …` argv;
pyflakes reports zero undefined names; the baseline tree's newest file is still 2026-07-26
(nothing written to it, no `__pycache__` added).

---

## R1 — REGRESSION (found and fixed)

**`download_and_vectorize.py` crashed on startup.** Its `__main__` block called
`configure_logging()`, which batch F introduced, but neither branch of the dual-import block
imported it. `python -m src.industrial_analysis.download_and_vectorize` died with
`NameError: name 'configure_logging' is not defined` before parsing a single argument — and that
module is **Step 1 of `industrial_analysis.sh`**, so the whole industrial pipeline was dead.

The dual-import pattern is exactly where this hides: a name added to one branch and not the other
fails only on the path that branch serves. Here it was missing from *both*.

Fixed by adding `configure_logging` to both branches of
[download_and_vectorize.py:31-49](../src/industrial_analysis/download_and_vectorize.py#L31-L49).
`pyflakes` across the whole tree now reports **zero undefined names**, so this was the only
instance. A `--help`-exits-0 sweep over all 45 `__main__` modules is what caught it; the test
suite did not, because the failing tests for that module were already in the 30-failure baseline.

---

## Behavior changes that are real and approved

| # | Change | Evidence | Classification |
|---|---|---|---|
| B1 | **`industrial_analysis_sweep.sh` was passing every value to the wrong flag.** Baseline emitted `--level 6 --version 13000 --buffer square_root --weight-method add --weight-func false --dynamic-buffering ''` — omitting `--version 2` shifted every later value one flag left, so the sweep ran with `buffer="square_root"` and `weight_func="false"`. Current emits `--version 2 --level 6 --buffer 13000 --weight-method square_root --weight-func add --dynamic-buffering false`. | Layer 2 argv diff, all 26 combinations | **intended** (E2) — far worse than the audit's "drops `--version`" |
| B2 | **`industrial_analysis.sh` Step 2 now runs.** Baseline never invoked `find_unconnected_industrial_areas`; current does. | Layer 2: a module invocation present only in current | **intended** (C1) |
| B3 | **`compare_pop_sweep_hw_eu.paths.eu_ref_filepath`** resolved to the literal string `nulls` (then to the path `<src>/nulls`); now resolves to the inherited `{data_dir}/extra_points/UWWTD_TreatmentPlants.gpkg`. | Layer 3: the only changed resolved value in 37 sections | **intended** (B1) |
| B4 | **`is_valid_geom` rejected every polygon.** `hasattr(polygon, "coords")` raises `NotImplementedError` under Shapely 2.1.2, and the blanket `except Exception` turned that into `False`. Baseline returns `False` for a clean unit square; current returns `True`. | Layer 4, recorded both ways | **intended** (G6) — but bigger than documented. The audit said the finite check "never runs"; in fact the function rejected all Polygon/MultiPolygon input. **Blast radius is zero**: `is_valid_geom` has no callers in either tree outside tests. |
| B5 | **Mosaic tile placement.** Baseline trimmed the tile with `data.shape[0]`/`shape[1]` — the band and row axes — where the destination window needs row/column extents. On an overhanging window it raises `ValueError: operands could not be broadcast together with shapes (1,2,3) (1,3,3)`; current places the tile correctly. | Layer 4, both outputs recorded | **intended** (G6) — changes mosaicked population rasters |
| B6 | **`auto_weight_scale` returned the mean, not the median.** `0.461234713` → `0.345988703` on a deliberately skewed fixture. | Layer 4 | **intended** (H4) |
| B7 | **Radius-prefixed zonal-sum columns.** `find_pop_in_danger_pop` emits `500_2020_zonal_sum`; all five baseline copies split on the *first* underscore and read year `500`, so a 2020 layer lost to a 2014 one. Baseline picks `2014_zonal_sum`, current picks `500_2020_zonal_sum`. | Layer 4 | **intended** (H3) — this is the latent shared bug the audit flagged |
| B8 | **`calculate_size` NaN guard (interactive piechart).** Baseline returned `nan` and put it in the SVG; current returns the minimum size. | Layer 4 | **intended** (H7) |
| B9 | **`pip install -e .` no longer runs on every invocation** (17 wrappers). Baseline used `install_package` unconditionally; current uses `ensure_src_importable`. | Layer 2 | **intended** (D7) |
| B10 | Composite histogram trimming quantiles move from the argparse defaults `0.05/0.95` to the config values `0.005/0.995`. | Layer 1 config diff (`composite_area_population_plots.plot_outlier_quantiles`) | **intended** (I4) |
| B11 | EU reference layer projection is estimated via `estimate_utm_crs` instead of hardcoded (3857 in `eu_comparison`, 32634 in the sweeps). | Layer 1: `eu_utm` removed, `_init_summary_worker`/`collect_metrics` signatures lost the parameter | **intended** (I4, "delete `eu_utm` entirely") |

## Behavior changes that were real decisions but were never written down

| # | Change | Assessment |
|---|---|---|
| U1 | **`correct_locations_w_OSM` swapped its country join.** Its own `enrich_country_with_duckdb` was deleted and the call now goes to `create_voronoi.intersects_with_country_db`. This is a sound deduplication, but it was **not on the plan's H1 site list**, and the two are not equivalent: the old join was `LEFT JOIN … ON ST_Intersects(a, b)` alone, while `intersects_with_country_db` additionally requires `a.LON_MIN >= b.LON_MIN AND a.LON_MAX <= b.LON_MAX AND …` — bbox *containment*, not bbox *overlap*. That predicate is pre-existing in `intersects_with_country_db` (it is in the baseline too), so this change propagated it to a new call site rather than introducing it. **Impact is low but non-zero**: it can only drop matches, never add them; for Point geometry it is a no-op; the affected input includes OSM *polygon* footprints, whose bbox is ~0.005° against a country bbox spanning degrees; and a dropped match degrades to the existing `CNTRY_ISO → alpha-2` fallback rather than to null. **Worth noting separately**: as a prefilter the predicate is simply wrong — the correct form is overlap (`a.LON_MIN <= b.LON_MAX AND a.LON_MAX >= b.LON_MIN …`). Fixing it would be a strict improvement at both call sites. Recommend raising it as its own item. |
| U2 | **`eu_comparison` now imports `orchestrate_single`/`composite_histogram` from `hw_comparison`** rather than keeping its own copies. The consolidated signature grew `qual_pop_default`, `filter_qual_pop`, `upper_quantile_hw_comp`, `comp_output_prefix`, `reference_name`, `hide_empty_axis` to carry the divergence explicitly. The two copies really did differ (the baseline had a dedicated test for the EU variant's `tight_layout` rect), so the consolidation had to pick a behavior and parameterize the rest. Sound, but it belongs in the H table and is not there. |
| U3 | **`industrial_analysis_sweep.sh` gained a `--shuffle-seed` flag.** Purely additive: `SHUFFLE_SEED="${SHUFFLE_SEED:-42}"` and the env-var mechanism are unchanged from the baseline, so an invocation that does not pass the flag behaves identically. |

## Confirmed unchanged

- **Resolved config**: the I4 key moves (`nodata_country_*` §35→§32, `plot_outlier_*` §35→§31,
  `annotations.random_seed` §9→§7, `mix_use_categories` to its earliest consumer) are all
  **inheritance-neutral** — 17 keys moved in the text, and exactly zero resolved values changed as a
  result. That is the strongest evidence the null-inheritance edits were safe.
- **`sample_annotations_by_class`** fails to resolve in *both* trees, with the identical
  `ConfigResolutionError`. Pre-existing and deliberately left (B3).
- **`execution.mode`** survived deletion correctly. I1 called it dead, but `create_voronoi.sh`
  reads it; it now carries a comment saying so.
- **H8 robust bounds**: `robust_bounds` matches `pop_at_risk_figures._robust_bounds` exactly
  (including the `positive_only` branch), and `clip_to_robust_bounds` matches composite's
  `clip_outliers`. The deliberate 1.5-vs-1.0 `iqr_factor` gap survived the consolidation.
- **H5 geometry repair**: `geo_utils.repair_geometry` matches the promoted baseline variant across
  valid, self-intersecting, non-polygon, empty and `None` input.
- **H7 `calculate_size`** (figure variant) matches the baseline on log-scale, degenerate-range and
  boundary inputs.
- **H3 zonal-sum resolver** picks the same column as the baseline for every well-formed input.
- **`is_valid_geom`** is unchanged for `None`, self-intersecting polygons, and `Point` with a NaN
  coordinate — the paths that already worked.
- **`cluster_point_indices`** still has two genuinely different implementations
  (`geo_utils.py` and `data_merge/final_data_merge.py`), matching the declined point-clustering item.
- **`NEW_04_EXPORTGEOTIFF`** fails to import in both trees, on the same hardcoded
  `H:/02_RESEARCH/...` paths. Pre-existing; already a deferred item.
- **No wrapper lost a module invocation or an override flag.** The seven standard flags are exposed
  by every entry point that takes them.

## Harness limitations worth knowing

- The wrapper argv harness stubs `-m` calls but delegates `-c` to a real interpreter. If the
  baseline is executed without `PYTHONPATH` pointing at it, its inline `from src... import` resolves
  to the **live installed package**, and the baseline appears to fail with the current code's errors.
  That produced one false regression before it was corrected — lay the baseline out as
  `<scratch>/src` and set `PYTHONPATH=<scratch>`.
- The pip stub makes `download_pop.sh` exit 1 on the baseline (its unconditional `install_package`
  is a no-op under the stub, so its follow-up import check fails). An artifact of stubbing, not a
  behavior difference.
- Layer 1's "first `set` line" heuristic mis-reads `lib/utils.sh`, which is a sourced library, not a
  wrapper — its `set -e` at line 75 is `run_stage` restoring state after `set +e`, and is correct.
- **Loading a baseline module writes `__pycache__` beside its source**, which edits a tree that is
  supposed to be read-only. The harnesses now set `sys.dont_write_bytecode` /
  `PYTHONDONTWRITEBYTECODE` before the first baseline import; a re-run leaves the baseline file
  count identical. No baseline `.py`, `.sh`, `.yaml` or `.md` was ever modified.
