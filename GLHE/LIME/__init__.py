import plotly.express as px
import os

px.set_mapbox_access_token(
    open(os.path.join("LocalData", ".mapbox_token")).read())
