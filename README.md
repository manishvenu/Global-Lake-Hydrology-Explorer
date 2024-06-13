# Global Lake Hydrology Explorer

Global Lake Hydrology Explorer is a for practice, in-development tool. If we can figure out all the gnarly details, it
is a tool for predicting future water levels of lakes around the world using global
precipitation, evapotranspiration, and runoff data. The tool visualizes all components of the water balance and uses a
small model component to simulate lake water levels. It allows users to select any lake in the world and obtain a
snapshot of its precipitation, evapotranspiration, and runoff.

**Disclaimer**: This project is written by an undergraduate student with limited knowledge in hydrology and programming.
It is not intended to be used for any serious research because it hasn't been validated. Please use it at your own risk.

## Current Status

- There are NO protections against bad arguments, so be careful!
- Local Data Folder is not uploaded to GitHub. When you run the code, it downloads the necessary data! THIS IS MASSIVE
  AND TAKES TIME! It's like 6 GB!
- The tool is currently in development. The current version is a prototype and is not ready for use.
- I don't know what coordinate systems are, but the goal is the general one, WCS_1984 or smthng

## Getting Started

### Installation

To use Global Lake Hydrology Explorer, you will need to install the required libraries and dependencies. Please see the
requirements.txt file for a list of required libraries and installation instructions. Once you set up the conda
environment it should just run. Remember, right now, runoff is only setup for CONUS lakes recognized in the NHDv2Plus.

### Usage

To use the tool in its current form, you will need your lakes' Hylak ID, you can find it
here: https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer. (The demo uses this website as well,
but the links only work when demo is active, but that requires ngrok and an account)
Write that into the 'GLHE/driver.py' file, and run the code. It takes about a minute to run after the first time, or
10-20 min
minutes the first time. (Ten minutes for ERA5 API), and you should see a zip file of a sample month of gridded
data, a csv of all the output data, and a plot of the all of it as well. The UI is also built, but requires the overall
demo to really function. See the logging file 'GLHE.log' to see current progress.

### Contributing

TBA, See Credits for where I've taken some code and things like that.

### License

Global Lake Hydrology Explorer is open source and distributed under the MIT License.

## Acknowledgments

Global Lake Hydrology Explorer was built using open-source libraries and datasets. We would like to acknowledge the
following sources: ERA5, CRUTS, HydroLakes, and more.

## Contact

If you have any questions or feedback, please feel free to contact me at manishrv@umich.edu.
