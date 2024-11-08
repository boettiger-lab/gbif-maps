import ibis
import streamlit as st
import os

base_url = "https://minio.carlboettiger.info"

# make sure h3 is installed.
import duckdb
db = duckdb.connect()
db.install_extension("h3", repository = "community")
db.close()





# enable ibis to use built-in function from the h3 extension
@ibis.udf.scalar.builtin
def h3_cell_to_boundary_wkt	(array) -> str:
    ...

# Configure write-access to source.coop
import streamlit as st
def set_source_secrets(con):
    con.raw_sql(query)
    source_key = st.secrets["SOURCE_KEY"]
    source_secret = st.secrets["SOURCE_SECRET"]
    
    query=   f'''
    CREATE OR REPLACE SECRET secret1 (
        TYPE S3,
        KEY_ID '{source_key}',
        SECRET '{source_secret}',
        ENDPOINT 'data.source.coop',
        URL_STYLE 'path'
    
    );
    '''

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
        URL_STYLE 'path'
    
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
            elevation_scale = 200 * v_scale,
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

#leafmap.basemap_xyz_tiles()
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
    "terrain": {"source": "terrainSource", "exaggeration": 2},
}