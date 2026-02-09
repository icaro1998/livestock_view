# GEE Catalog Snapshot

`gee_catalog.csv` is a pinned snapshot of allowed Earth Engine dataset IDs for this repository.

## Rules
- Treat this file as canonical when writing notebooks/scripts.
- Update it intentionally (in a separate commit) when adding/removing dataset IDs.
- Do not introduce ad-hoc IDs in analysis code without first updating this snapshot.

## Quick Load Snippet
```python
import pandas as pd

catalog = pd.read_csv("data/catalog/gee_catalog.csv")
print(catalog[["dataset_id", "kind"]].head())

# Example filters
s1 = catalog[catalog["dataset_id"].str.contains("S1", case=False)]
collections = catalog[catalog["kind"] == "ImageCollection"]
print(len(collections), "collections")
```

## Update Checklist
1. Add/modify IDs in `gee_catalog.csv`.
2. Document why the change is needed in the commit message.
3. Re-run notebook/script checks that rely on catalog IDs.
