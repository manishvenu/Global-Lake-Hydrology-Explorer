import GLHE


def main():
    HYLAK_ID = 67
    clay_driver_obj = GLHE.CLAY.CLAY_driver.CLAY_driver()
    output_files_dir = clay_driver_obj.main(HYLAK_ID)

    # Add output_files_dir to the LIME/config/config.json file or pass here.

    lime_driver_obj = GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_files_dir)

    # Open the output website


if __name__ == "__main__":
    main()
