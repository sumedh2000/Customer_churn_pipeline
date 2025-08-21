$ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
git rev-parse --short HEAD > .git_sha.txt
$sha = Get-Content .git_sha.txt
Remove-Item .git_sha.txt

@"
# Data Version Log

- Timestamp: $ts
- Git commit: $sha
- Tracked:
  - raw_data  → raw_data.dvc
  - data/clean  → data/clean.dvc
  - data/transformed  → data/transformed.dvc
  - data/feature_sets  → data/feature_sets.dvc

Notes:
- Data added/changed per earlier stages (2–7).
"@ | Out-File -FilePath reports/data_version_log.md -Encoding utf8 -Append

git add reports/data_version_log.md
git commit -m "Stage 8: Record data version snapshot"
