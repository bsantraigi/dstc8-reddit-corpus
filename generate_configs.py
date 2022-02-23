prefix = \
"""run_dir: "scratch/prod/{yr}/"
min_year: "{yr}"
max_year: "{yr}"
min_year_min_month: 1
max_year_max_month: 12
data_zip_path: dstc8-reddit-corpus-{yr}.zip"""

for yr in range(2010, 2021):
    with open(f"configs/prod/{yr}.yaml", "w") as f:
        f.write(prefix.format(yr=yr))
