# make sure h3 is installed.
import duckdb
db = duckdb.connect()
db.install_extension("h3", repository = "community")
db.close()

import ibis

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


import pydeck as pdk

def HexagonLayer(data):
    return pdk.Layer(
            "H3HexagonLayer",
            id="gbif",
            data=data,
            extruded=True,
            get_elevation="value",
            get_hexagon="hex",
            elevation_scale=50,
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

#deck.to_html("gbif.html")
