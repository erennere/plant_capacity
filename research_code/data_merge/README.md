# data_merge

This folder builds the canonical WWTP point dataset that the rest of the repository depends on. It combines geometry correction, segmentation integration, and final multi-source merge logic into one reproducible chain. If this stage is wrong, all later Voronoi and risk outputs are affected, so this is typically the first stage you run. Outputs are written to config-driven paths in `research_code/config.yaml`.

## How This Folder Is Run

From `research_code/`:

```bash
bash data_merge/combine_locations.sh
```

## Python Scripts (Logic)

### correct_locations_w_OSM.py
Aim: Correct WWTP geometries using nearby OSM candidates and rule-based matching. Inputs: Base points, OSM-derived candidates, and correction parameters. Outputs: Corrected south subset with updated geometry/provenance. How: It performs nearest/valid matching and writes corrected geospatial output. It only applies this OSM correction to WWTPs which cannot be found aerially at their given location. 'rad' parameter determines the radius of search for OSM.
 The rule is:
- if a model was able to correct the geometry of a given WWTP, this will be used
- if a model was able to find extra locations, this will be taken as such
- if a model wasn't able to detect the WWTPs at their location, OSM database will be searched within given radius, if found this new location will be used, else discarded. 

### merge_seg_results.py
Aim: Merge segmentation outputs into corrected point datasets. Inputs: Segmentation CSV and corrected point layers. Outputs: Segmentation-enriched outputs for legacy and current variants. How: It parses model outputs, aligns by keys/indices, and updates/writes merged files. 'legacy_merge' can be deactivated in config.yaml which you should do if you run this repo for the first time. 

### final_data_merge.py
Aim: Produce the final all-country WWTP layer for downstream stages. Inputs: Corrected layers plus country-specific source files. Outputs: Consolidated final dataset at corrected_all_filepath. How: It harmonizes columns, merges source datasets, resolves duplicates, and writes final output.

## Shell Scripts (Entry Points)

### combine_locations.sh
Aim: Run the full merge chain in production order. Inputs: Config defaults and optional CLI overrides. Outputs: Stage logs and final merged WWTP dataset. How: It executes correction, segmentation merge (old/new variants), and final merge sequentially.

## Shell -> Python Flow Diagram

```text
combine_locations.sh
	-> correct_locations_w_OSM.py
	-> merge_seg_results.py (old variant)
	-> final_data_merge.py
	-> merge_seg_results.py (new variant)
```
