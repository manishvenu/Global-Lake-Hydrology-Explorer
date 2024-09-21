File Organization
=================
The Global Lake Hydrology Explorer is organized as follows:
 - The folder 'GLHE' has all of the code
 - The folder 'GLHE/data_acess' has the individual product data access functions. They accept shapely polygons and return raw xarray datasets. I'm not sure if I should include minor modifations to the dataset here, but I'm thinking no.
 - The file 'GLHE/lake_extraction.py' has the lake extraction functions. It accepts HyLak_Id's and returns shapely polygons. It also accepts xarray datasets and those polygons and subsets the datasets
 - The file 'GLHE/product_driver_functions.py' has the product driver functions for each seperate product. It does all the adjustments to the data needed and converts them into a wrapped Pandas Series class (MVSeries) It calls the data_access functions and the lake_extraction functions.
 - The file 'GLHE/combined_data_functions.py' merges the datasets and presents them to the user. It runs with pandas.
 - The file 'GLHE/driver.py' is the main file. It calls the product_driver, and combined_data_functions files and runs the explorer.
