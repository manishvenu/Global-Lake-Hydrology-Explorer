import plotly.express as px
import os
from pathlib import Path

px.set_mapbox_access_token(
    open(os.path.join(Path(__file__).parent, "LocalData", ".mapbox_token")).read()
)

from . import LIME_sample_dashboard
