import ibis
from ibis import _

## unique names from the polygon databases

(ibis
.duckdb.connect()
.read_geo("/vsicurl/https://huggingface.co/datasets/cboettig/biodiversity/resolve/main/data/NPS.gdb")
.filter(_.UNIT_TYPE.isin(["National Park", "National Seashore", "National Wild & Scenic River", "National Monument"]))
.select("PARKNAME", "UNIT_NAME", "UNIT_TYPE")
.rename(name = "UNIT_NAME")
.distinct()
.order_by("name")
.mutate(index = ibis.row_number())
.to_parquet("park_names.parquet")
)



(ibis
.duckdb.connect()
.read_geo("/vsicurl/https://github.com/nvkelso/natural-earth-vector/raw/master/geojson/ne_110m_admin_1_states_provinces.geojson")
.select("name")
.distinct()
.order_by("name")
.mutate(index = ibis.row_number())
.to_parquet("state_names.parquet")
)

(ibis
.duckdb.connect()
.read_geo("/vsicurl/https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_scale_rank.geojson")
.filter(_.scalerank == 1)
.rename(name = "sr_subunit")
.select("name")
.distinct()
.order_by("name")
.mutate(index = ibis.row_number())
.to_parquet("country_names.parquet")
)


