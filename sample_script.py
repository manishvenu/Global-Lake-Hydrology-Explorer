import GLHE


def main():
    # Get your lake id from the below website
    HYLAK_ID = 67  # Get Lake ID from here: https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer
    
    # Start the Data Access (CLAY) Driver
    clay_driver_obj = GLHE.CLAY.CLAY_driver.CLAY_driver()
    
    # Run the Driver with the lake id
    output_files_dir = clay_driver_obj.main(HYLAK_ID)

    # Run the visualization, which is not OOP (yet). Add output_files_dir to the LIME/config/config.json file or pass as shown here.

    lime_driver_obj = GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_files_dir)

    # Open the output website, because we run a website, this function won't finish, it'll just point to somewhere on the local host.


if __name__ == "__main__":
    main()
