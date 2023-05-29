from GLHE import lake_extraction, product_driver_functions, all_data_actions


def driver() -> None:
    """
    This is the driver function for the all_data_actions.py file.
    Here, we'll access all the data
    """

    # Select Lake and Extract Polygon
    hylak_id = 798
    lake_polygon = lake_extraction.extract_lake(hylak_id)

    # Get Data with polygon
    ERA5_Datasets = product_driver_functions.ERA_Land_driver(lake_polygon)
    CRUTS_Datasets = product_driver_functions.CRUTS_driver(lake_polygon)
    print(*ERA5_Datasets)
    print(*CRUTS_Datasets)

    # Compile Data
    pandas_dataset = all_data_actions.merge_mv_series(*ERA5_Datasets, *CRUTS_Datasets)

    # Plot and Output
    all_data_actions.plot_all_data(pandas_dataset)
    all_data_actions.output_all_compiled_data_to_csv(pandas_dataset, "test.csv")

if __name__ == "__main__":
    driver()
