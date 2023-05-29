# Global Lake Hydrology Explorer
Global Lake Hydrology Explorer is a tool for predicting future water levels of lakes around the world using global precipitation, evapotranspiration, and runoff data. The tool visualizes all components of the water balance and uses a small model component to simulate lake water levels. It allows users to select any lake in the world and obtain a snapshot of its precipitation, evapotranspiration, and runoff.

**Disclaimer**: This project is written by a undergraduate student with limited knowledge in hydrology and programming. It is not intended to be used for any serious research. Please use it at your own risk.

## Current Status
 - Local Data Folder is not uploaded to GitHub. Please contact me for access to the data folder.
 - The tool is currently in development. The current version is a prototype and is not ready for use.
 - I don't know what coordinate systems are
 - For UI, the plan is either ArcGIS (hopefully not) or some website hosted by UMUCH
## Getting Started
### Installation
To use Global Lake Hydrology Explorer, you will need to install the required libraries and dependencies. Please see the requirements.txt file for a list of required libraries and installation instructions.

### Usage
To use the tool in its current form, you will need your lakes' Hylak ID, you can find it here: https://hub.arcgis.com/maps/0abb136c398942e080f736c8eb09f5c4/explore
Write that into the 'driver.py' file, and run the code. It takes about a minute to run after the first time, or 11 minutes the first time if the ERA5 dataset hasn't been called before
You can find the output data in the .temp folder and you should see a matplotlib plot pop up at the end. You can also see the logging file 'GLHE.log' to see current progress.

### Contributing
TBA, See Credits for code stealing and things like that.

### License
Global Lake Hydrology Explorer is open source and distributed under the MIT License.

## Acknowledgments
Global Lake Hydrology Explorer was built using open-source libraries and datasets. We would like to acknowledge the following sources: ERA5, CRUTS, Hydrolakes, and more.

## Contact
If you have any questions or feedback, please feel free to contact me at manishrv@umich.edu.
