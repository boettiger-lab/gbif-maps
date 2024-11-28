import os

base_url = "https://minio.carlboettiger.info"

# make sure h3 is installed.
import duckdb
db = duckdb.connect()
db.install_extension("h3", repository = "community")
db.close()

# enable ibis to use built-in function from the h3 extension
import ibis
from ibis import _
@ibis.udf.scalar.builtin
def h3_cell_to_boundary_wkt	(array) -> str:
    ...


# Configure write-access to source.coop
import streamlit as st
def set_source_secrets(con):
    source_key = st.secrets["SOURCE_KEY"]
    source_secret = st.secrets["SOURCE_SECRET"]
    
    query=   f'''
    CREATE OR REPLACE SECRET source (
        TYPE S3,
        KEY_ID '{source_key}',
        SECRET '{source_secret}',
        ENDPOINT 'data.source.coop',
        URL_STYLE 'path'
    );
    set THREADS=40;
    '''


    con.raw_sql(query)

def set_aws_secrets(con):    
    query=   f'''
    CREATE OR REPLACE SECRET aws (
        TYPE S3,
        ENDPOINT 's3.us-west-2.amazonaws.com',
        SCOPE 's3://overturemaps-us-west-2/release/'
    );
    '''

    # ENDPOINT 'data.source.coop',

    con.raw_sql(query)

# or write access to minio
def set_secrets(con):   
    minio_key = st.secrets["MINIO_KEY"]
    minio_secret = st.secrets["MINIO_SECRET"]
    query=   f'''
    CREATE OR REPLACE SECRET secret2 (
        TYPE S3,
        KEY_ID '{minio_key}',
        SECRET '{minio_secret}',
        ENDPOINT 'minio.carlboettiger.info',
        URL_STYLE 'path',
        SCOPE 's3://cboettig/gbif'
    );
    '''
    con.raw_sql(query)


import minio
def s3_client(type="minio"):
    minio_key = st.secrets["MINIO_KEY"]
    minio_secret = st.secrets["MINIO_SECRET"]
    client = minio.Minio("minio.carlboettiger.info", minio_key, minio_secret)
    if type == "minio":
        return client

    source_key = st.secrets["SOURCE_KEY"]
    source_secret = st.secrets["SOURCE_SECRET"]
    client = minio.Minio("data.source.coop", source_key, source_secret)
    return client
    

import pydeck as pdk
def HexagonLayer(data, v_scale = 1):
    return pdk.Layer(
            "H3HexagonLayer",
            id="gbif",
            data=data,
            extruded=True,
            get_elevation="value",
            get_hexagon="hex",
            elevation_scale = 50 * v_scale,
            elevation_range = [0,1],
            pickable=True,
            auto_highlight=True,
            get_fill_color="[255 - value, 255, value]",
            )

def DeckGlobe(layer):
    view_state = pdk.ViewState(latitude=51.47, longitude=0.45, zoom=0)
    view = pdk.View(type="_GlobeView", controller=True, width=1000, height=600)
    COUNTRIES = "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_scale_rank.geojson"
    
    layers = [
        pdk.Layer(
            "GeoJsonLayer",
            id="base-map",
            data=COUNTRIES,
            stroked=False,
            filled=True,
            get_fill_color=[200, 200, 200],
        ),
        layer,
    ]
    deck = pdk.Deck(
        views=[view],
        initial_view_state=view_state,
        layers=layers,
        map_provider=None,
        # Note that this must be set for the globe to be opaque
        parameters={"cull": True},
    )
    return deck

key = st.secrets['MAPTILER_KEY']
terrain_style = {
    "version": 8,
    "sources": {
        "osm": {
            "type": "raster",
            "tiles": ["https://server.arcgisonline.com/ArcGIS/rest/services/NatGeo_World_Map/MapServer/tile/{z}/{y}/{x}.png"],
            "tileSize": 256,
            "attribution": "&copy; National Geographic",
            "maxzoom": 19,
        },
        "terrainSource": {
            "type": "raster-dem",
            "url": f"https://api.maptiler.com/tiles/terrain-rgb-v2/tiles.json?key={key}",
            "tileSize": 256,
        },
        "hillshadeSource": {
            "type": "raster-dem",
            "url": f"https://api.maptiler.com/tiles/terrain-rgb-v2/tiles.json?key={key}",
            "tileSize": 256,
        },
    },
    "layers": [
        {"id": "osm", "type": "raster", "source": "osm"},
        {
            "id": "hills",
            "type": "hillshade",
            "source": "hillshadeSource",
            "layout": {"visibility": "visible"},
            "paint": {"hillshade-shadow-color": "#473B24"},
        },
    ],
    "terrain": {"source": "terrainSource", "exaggeration": .1},
}
####



## grab polygon of a National park:
import ibis
from ibis import _
def get_national_park(name = "Yosemite National Park", con = ibis.duckdb.connect()):
    gdf = (con
        .read_geo("/vsicurl/https://huggingface.co/datasets/cboettig/biodiversity/resolve/main/data/NPS.gdb")
        .filter(_.UNIT_NAME == name)
        .select(_.SHAPE)
        .mutate(SHAPE = _.SHAPE.convert('EPSG:3857', 'EPSG:4326'))
       ).execute()
    return gdf

# ick consider using data from Overture
def get_country(name = "United States", con = ibis.duckdb.connect()):
    gdf = (con
        .read_geo("/vsicurl/https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_scale_rank.geojson")
        .filter(_.sr_subunit == name, _.scalerank == 1)
        .select(_.geom)
       ).execute()
    return gdf


def get_state(name = "California", con = ibis.duckdb.connect()):
    gdf = (con
        .read_geo("/vsicurl/https://github.com/nvkelso/natural-earth-vector/raw/master/geojson/ne_110m_admin_1_states_provinces.geojson")
        .filter(_.name == name)
        .select(_.geom)
       ).execute()
    return gdf

import geopandas as gpd
def get_city(name = "Oakland", con = ibis.duckdb.connect()):
    gdf = (con
        .read_geo("/vsicurl/https://data.source.coop/cboettig/us-boundaries/mappinginequality.json")
        .filter(_.city == name)
        .agg(geom = _.geom.unary_union())
       ).execute()
    gdf = gpd.GeoDataFrame(geometry=gdf.simplify(.05))
    return gdf 


@st.cache_data
def get_polygon(name = "Yosemite National Park", 
                source = "National Parks",
                _con = ibis.duckdb.connect()):
    match source:
        case 'National Parks':
            gdf = get_national_park(name, _con)
        case 'States':
            gdf = get_state(name, _con)
        case 'Countries':
            gdf = get_country(name, _con)
        case 'Cities':
            gdf = get_city(name, _con)
        case 'Custom':
            gdf = gpd.read_file(name)
        case "World":
            gdf = None
        case _:
            gdf = None
    return gdf

import hashlib
import pandas as pd
def unique_path(gdf_name, rank, taxa, zoom, distinct_taxa):
    #gdf_hash = str(pd.util.hash_pandas_object(gdf).sum())
    text = gdf_name + rank + taxa + str(zoom) + distinct_taxa
    hash_object = hashlib.sha1(text.encode())
    sig = hash_object.hexdigest()
    dest = "cache/gbif_" + sig + ".json"
    return dest


