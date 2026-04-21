# pop_validation_scripts

This folder checks whether pipeline outputs are plausible and consistent with known reference datasets. It is best treated as a quality gate after major runs, especially when parameters or input data changed. The scripts focus on HydroWaste and EU comparisons plus broad verification summaries. Outputs are diagnostic tables and logs used to detect regressions.

## How This Folder Is Run

From `research_code/`:

```bash
bash pop_validation_scripts/comparison.sh
```

Or run modules directly:

```bash
python -m research_code.pop_validation_scripts.verification_script
python -m research_code.pop_validation_scripts.hw_comparison
python -m research_code.pop_validation_scripts.eu_comparison
```

## Python Scripts (Logic)

### verification_script.py
Aim: Perform general validation checks on produced outputs. Inputs: Pipeline outputs and configured reference layers. Outputs: Validation summaries and mismatch diagnostics. How: It aligns keys/geometries across datasets and computes verification metrics.

### hw_comparison.py
Aim: Compare outputs against HydroWaste references. Inputs: Pipeline WWTP outputs and HydroWaste layers/tables. Outputs: HydroWaste-focused comparison metrics and discrepancy reports. How: It executes spatial/key comparisons and exports diagnostics.

### eu_comparison.py
Aim: Compare outputs against European WWTP references. Inputs: Pipeline outputs and EU reference datasets. Outputs: EU agreement/mismatch summaries and supporting tables. How: It harmonizes identifiers and runs targeted comparison metrics.

## Shell Scripts (Entry Points)

### comparison.sh
Aim: One-command launcher for validation comparisons. Inputs: Config defaults and optional overrides. Outputs: Validation logs and outputs from comparison modules. How: It runs verification and comparison scripts in sequence.

## Shell -> Python Flow Diagram

```text
comparison.sh -> verification_script.py -> hw_comparison.py -> eu_comparison.py
```
