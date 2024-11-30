import streamlit as st

import ibis
from ibis import _
import pydeck as pdk
from utilities import *
import leafmap.maplibregl as leafmap
import requests
import geopandas as gpd

st.set_page_config(page_title="GBIF Observations Explorer", layout="wide")
st.title("GBIF Observations Explorer")



con = ibis.duckdb.connect(extensions=['httpfs', 'spatial', 'h3'])
set_secrets(con) # s3 credentials
set_aws_secrets(con)
#set_source_secrets(con)

distinct_taxa = "" # default

col1, col2, col3, col4 = st.columns([1,3,3,3])

# placed outside the form so that toggling this immediately updates the form options available
with col1:
    st.markdown("#### Start 👇")
    area_source = st.radio("Area types", 
                           ["National Parks", 
                            "Cities", 
                            "States", 
                            "Countries",
                            "World",
                            "Custom"]) 
    nunique = st.toggle("unique taxa only", False)


# config with different default settings by area
config = {
    "National Parks": {
        "names": con.read_parquet("park_names.parquet").select("name").execute(),
        "index": 161,
        "zoom": 7,
        "vertical": 1.0,
        "rank_index": 2,
        "taxa": "Aves",
    },
    "Cities": {
        "names": con.read_parquet("city_names.parquet").select("name").execute(),
        "index": 183,
        "zoom": 11,
        "vertical": 0.1,
        "rank_index": 2,
        "taxa": "Aves",

    },
    "States": {
        "names": con.read_parquet("state_names.parquet").select("name").execute(),
        "index": 4,
        "zoom": 5,
        "vertical": 2.0,
        "rank_index": 2,
        "taxa": "Aves",       
    },
    "Countries": {
        "names": con.read_parquet("country_names.parquet").select("name").execute(),
        "index": 183,
        "zoom": 4,
        "vertical": 4.0,
        "rank_index": 2,
        "taxa": "Aves",        
    },
     "World": {
        "names": ["World"],
        "index": 0,
        "zoom": 6,
        "vertical": 4.0,
        "rank_index": 6,
        "taxa": "Balaenoptera musculus",
    },

    "Custom": {
        "zoom": 5,
        "vertical": 2.0,
        "rank_index": 2,
        "taxa": "Aves",      
    }
    
}

with st.form("my_form"):
    
    taxonomic_ranks = ["kingdom", "phylum", "class", "order", "family","genus", "species"]
    default = config[area_source]
    
    with col2:
        st.markdown("#### 🗺️ Select an area of interest")
        if area_source == "Custom":
            geo_file = st.file_uploader("Polygon in EPSG:4326")
            
        else:
            gdf_name = st.selectbox("Area", default["names"], index=default["index"])
    
    with col3:
        st.markdown("#### 🐦 Select taxonomic groups")
        ## add support for multiple taxa!
        rank = st.selectbox("Taxonomic Rank", options=taxonomic_ranks, index = default["rank_index"])
        taxa = st.text_input("taxa", default["taxa"])
        if nunique:
            distinct_taxa = st.selectbox("Count only unique occurrences by:", options=taxonomic_ranks, index = default["rank_index"])

    with col4: 
        st.markdown('''
        #### 🔎 Set spatial resolution
        See [H3 cell size by zoom](https://h3geo.org/docs/core-library/restable/#cell-areas)
        ''')
        zoom = st.slider("H3 resolution", min_value=1, max_value=11, value = default["zoom"])
        v_scale = st.number_input("vertical scale", min_value = 0.0, value = default["vertical"])

    submitted = st.form_submit_button("Go")

@st.cache_data
def compute_hexes(_gdf, gdf_name, rank, taxa, zoom, distinct_taxa = ""):

    # FIXME check if dest exists in cache
    dest = unique_path(gdf_name, rank, taxa, zoom, distinct_taxa)
    bucket = "cboettig/gbif"
    url = base_url + "/cboettig/gbif/" + dest

    response = requests.head(url)
    if response.status_code != 404:
        return url


    sel = con.read_parquet("s3://cboettig/gbif/2024-10-01/**")

    if gdf is not None:
        # Much faster to filter to bbox first, then polygon
        geo_column = gdf.geometry.name
        bounds =  _gdf.buffer(1).total_bounds
        wkt = ibis.literal(_gdf[geo_column].iloc[0])
        sel = (sel
               .filter(_.decimallongitude >= bounds[0], 
                       _.decimallongitude < bounds[2], 
                       _.decimallatitude >= bounds[1], 
                       _.decimallatitude < bounds[3])
               .filter( _.geom.within(wkt))
             )

    sel = sel.filter(_[rank].isin([taxa]))
    
    sel = (sel
           .rename(hex = "h" + str(zoom)) # h3 == 41,150 hexes.  h5 == 2,016,830 hexes
           .group_by(_.hex)
           )

    if distinct_taxa != "": # count n unique taxa
        sel = sel.agg(n = _[distinct_taxa].nunique()) 
    else: # count occurrences
        sel = sel.agg(n = _.count())

    sel = (sel
           .filter(_.n > 0)
           .mutate(logn = _.n.log())
           .mutate(value = (255 * _.logn / _.logn.max()).cast("int")) # normalized color-scale
    )

    # .to_json() doesn't exist in ibis, use SQL
    query = ibis.to_sql(sel)
    con.raw_sql(f"COPY ({query}) TO 's3://{bucket}/{dest}' (FORMAT JSON, ARRAY true);")

    return url






mappinginequality = 'https://data.source.coop/cboettig/us-boundaries/mappinginequality.pmtiles'

redlines = {'version': 8,
 'sources': {'source': {'type': 'vector',
   'url': 'pmtiles://' + mappinginequality,
   'attribution': 'PMTiles'}},
 'layers': [{'id': 'mappinginequality_fill',
   'source': 'source',
   'source-layer': 'mappinginequality',
   'type': 'fill',
   'paint': {'fill-color': ["get", "fill"], 'fill-opacity': 0.9},}
    ]}



if submitted:
    gdf = get_polygon(gdf_name, area_source, con)
    url = compute_hexes(gdf, gdf_name, rank, taxa, zoom, distinct_taxa = distinct_taxa)
    
    layer = HexagonLayer(url, v_scale)
    m = leafmap.Map(style= terrain_style, center=[-120, 37.6], zoom=2, pitch=35, bearing=10)

    if gdf is not None:
        m.add_gdf(gdf[[gdf.geometry.name]], "fill", paint = {"fill-opacity": 0.2}) # adds area of interest & zooms in

    if area_source == "Cities":
        m.add_pmtiles(mappinginequality, style=redlines, visible=True, opacity = 0.9,  fit_bounds=False)

    m.add_deck_layers([layer])
    m.add_layer_control()
    m.to_streamlit()
