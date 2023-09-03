import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

x, y = polygon.exterior.coords.xy
x = list(x)
y = list(y)
lats = lo_nwm.latitude
lons = lo_nwm.longitude
df = pd.DataFrame({"x": lons, "y": lats})
s = px.scatter_mapbox(df, lat="y", lon="x")
r = go.Scatter(x=x, y=y, fill='toself')
