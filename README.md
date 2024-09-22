# Global Lake Hydrology Explorer

Global Lake Hydrology Explorer is a for practice, in-development tool. If implemented, it
is a tool for predicting future water levels of lakes around the world using global
precipitation, evapotranspiration, and runoff data. The tool visualizes all components of the water balance and uses a
small model component to simulate lake water levels. It allows users to select any lake in the world and obtain a
snapshot of its precipitation, evapotranspiration, and runoff.

**Disclaimer**: This project is written by a undergrad student with limited knowledge in hydrology and programming.
It is not intended to be used for any serious research because it hasn't been validated. Please use it at your own risk.

## Current Status

- Haven't implemented good remote file acces with zarr or anything, so everything is downloaded. The Local Data Folder is not uploaded to GitHub. When you run the code, it downloads the necessary data! THIS IS MASSIVE AND TAKES TIME! It's like 20 GB!
- The tool is currently in development. The current version is a protoype.
- I don't know what coordinate systems are, but the goal is WCS_1984 for projection.
- LIME doesn't really work, it's more about CLAY right now.

## Getting Started

### Installation

To use Global Lake Hydrology Explorer, please:

1. Clone the Repo on a LINUX computer (Windows/Mac is fine, just need to workshop the packages a little.)
2. You will need to setup the environment. The suggested install is with conda, and requirements/env-GLHE.yml or requirements/env-GLHE-basic.yml which should give some flexibility as the conda environment file. You can also use the requirements.txt files, which may or may not be out of date.
3. Once you set up the conda
environment it should just run. FYI, right now, runoff is only setup for CONUS lakes recognized in the NHDv2Plus.

### Usage

Check out sample_script.py! To use the tool in its current form, it's a package, so you can import it like a regular package from the main folder. The package is split into three subpackages, CLAY, LIME, and CALCITE, under the GLHE hood. You call CLAY to do the data access and LIME to visualize it (Similar to AWIPS!). CALCITE is just a message transfer service that doesn't need to be touched, but you can tap into it to get metadata if wanted. Check out the files.

You will need your lakes' HYLAK ID, you can find it
here: https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer. Ignore all the demo links, which only work in a supervised environment. 

Import GLHE, call the CLAY_driver, which will download all the data the first time (OR FOR FASTER DOWNLOAD - go here: https://www.dropbox.com/scl/fo/d9a8t8rs05qjr9dnzdzvw/AOIR2cebgd4tb3u5rjOm4RQ?rlkey=z8pd6py7ec1wwwt0rahae77zb&st=c10pjgqf&dl=0, download the folder GLHE-Large Data REpository (20 GB) and extract it in GLHE/CLAY as "LocalData" folder, which is faster), then run the CLAY_driver, and then pass the output folder location of CLAY_driver to LIME. You can change the output folder location in GLHE.CLAY.globals!

This is tested to work on Linux and Windows, but Mac shouldn't be a problem. We're not using OS-specific packages yet. 
### Contributing

TBA, See Credits for where I've taken some code and things like that. I have taken A LOT of code from random sources hah.

### License

Global Lake Hydrology Explorer is open source and distributed under the MIT License.

## Acknowledgments

Global Lake Hydrology Explorer was built using open-source libraries and datasets. We would like to acknowledge the
following sources: ERA5, CRUTS, HydroLakes, and more.

## Contact

If you have any questions or feedback, please feel free to contact me at manishrv@umich.edu.
