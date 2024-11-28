import streamlit as st

import ibis
from ibis import _
import pydeck as pdk
from utilities import *
import leafmap.maplibregl as leafmap
import requests


st.set_page_config(page_title="GBIF Observations Explorer", layout="wide")




con = ibis.duckdb.connect(extensions=['httpfs', 'spatial', 'h3'])
set_secrets(con) # s3 credentials
#set_source_secrets(con)

distinct_taxa = "" # default

col1, col2, col3, col4 = st.columns([1,3,3,3])

# placed outside the form so that toggling this immediately updates the form options available
with col1:
    nunique = st.toggle("unique taxa", False)
    area_source = st.radio("Area types", 
                           ["National Parks", 
                            "Cities", 
                            "States", 
                            "Countries"]) 



area_selector = {
    "National Parks": {
        "names": con.read_parquet("park_names.parquet").select("name").execute(),
        "index": 161,
        "zoom": 7,
        "default_v": 1.0,
    },
    "Cities": {
        "names": con.read_parquet("city_names.parquet").select("name").execute(),
        "index": 183,
        "zoom": 11,
        "default_v": 0.1,


    },
    "States": {
        "names": con.read_parquet("state_names.parquet").select("name").execute(),
        "index": 4,
        "zoom": 5,
        "default_v": 2.0,
        
    },
    "Countries": {
        "names": con.read_parquet("country_names.parquet").select("name").execute(),
        "index": 183,
        "zoom": 4,
        "default_v": 4.0,

    },
    
}

with st.form("my_form"):
    
    taxonomic_ranks = ["kingdom", "phylum", "class", "order", "family","genus", "species"]
    area_info = area_selector[area_source]
    
    with col2:
        gdf_name = st.selectbox("Area", area_info["names"], index=area_info["index"])
    
    with col3:
        rank = st.selectbox("Taxonomic Rank", options=taxonomic_ranks, index = 2)
        taxa = st.text_input("taxa", "Aves")
        if nunique:
            distinct_taxa = st.selectbox("count unique", options=taxonomic_ranks, index = 6)

    with col4: 
        zoom = st.slider("H3 resolution", min_value=1, max_value=11, value = area_info["zoom"])
        v_scale = st.number_input("vertical scale", min_value = 0.0, value = area_info["default_v"])

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
    
    # Much faster to filter to bbox first, then polygon
    geo_column = gdf.geometry.name
    bounds =  _gdf.buffer(1).total_bounds
    wkt = ibis.literal(_gdf[geo_column].iloc[0])
    sel = (con
           .read_parquet("s3://cboettig/gbif/2024-10-01/**")
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

    m.add_gdf(gdf[[gdf.geometry.name]], "fill", paint = {"fill-opacity": 0.2}) # adds area of interest & zooms in

    if area_source == "Cities":
        m.add_pmtiles(mappinginequality, style=redlines, visible=True, opacity = 0.9,  fit_bounds=False)

    m.add_deck_layers([layer])
    m.add_layer_control()
    m.to_streamlit()
