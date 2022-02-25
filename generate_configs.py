prefix = \
"""run_dir: "scratch/prod_{yr}/"
max_concurrent_downloads: 4
min_year: "{yr}"
max_year: "{yr}"
min_year_min_month: 1
max_year_max_month: 12
data_zip_path: dstc8-reddit-corpus-{yr}.zip
"""

with open("configs/prod/template_2016.yaml") as f:
    postfix = f.read()

for yr in range(2005, 2022):
    with open(f"configs/prod/{yr}.yaml", "w") as f:
        f.write(prefix.format(yr=yr) + postfix)
