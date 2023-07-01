import json
import logging
import os
import json
import requests
from zipfile import ZipFile

logger = logging.getLogger(__name__)


# THIS MODULE CHECKS FOR DATA BY FILENAME!!!!! IF YOU CHANGE THE FILENAME, YOU MUST CHANGE THE FILENAME IN THE CONFIGURATION FILE
def download_data_from_dropbox(dropbox_link: str, filename: str, is_folder: bool) -> None:
    """
    This function downloads data from dropbox. It is used to download data from dropbox that is too large to be shipped with the project.
    Parameters
    ----------
    dropbox_link : str
        The dropbox link to the data
    filename : str
        The filename of the data
    is_folder : bool
        If the data is a folder or not
    Returns
    -------
    None
    """
    logger.info("** Checking Data **")
    headers = {'user-agent': 'Wget/1.16 (linux-gnu)'}
    r = requests.get(dropbox_link, stream=True, headers=headers)
    filepath = "LocalData/" + filename
    if is_folder:
        filepath += ".zip"
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    if is_folder:
        with ZipFile(filepath, "r") as zip_file:
            # Extract the files
            zip_file.extractall("LocalData/" + filename)
        os.remove(filepath)
    logger.info("Finished downloading " + filename + " from dropbox")
    return


def check_data_and_download_missing_data_or_files() -> None:
    """
    This function checks if data files (listed in configuration file) exists in the LocalData folder.
    Returns
    -------
    list[str]
        Returns data files that need to be imported
    """

    # Check if Local Data Folder Exists #
    if not os.path.exists("LocalData"):
        os.mkdir("LocalData")

    # Read in local data files list #
    with open("data_access\data_access_config\input_data.json") as f:
        input_data_config = json.load(f)

    data_products = input_data_config['data_products']

    for product in data_products:
        name = product['name']
        code = product['access_code']
        if code == "local":
            filename = product['local_remote_storage_filename']
            is_folder = False
            if not os.path.exists("LocalData/" + filename):
                logging.info("Downloading " + name + " data. It will take some time!")
                if filename[-1] == '/':
                    is_folder = True
                    # os.mkdir("LocalData/" + filename[:-1])
                    download_data_from_dropbox(product['dropbox_download_link'], filename[:-1], is_folder)
                else:
                    download_data_from_dropbox(product['dropbox_download_link'], filename, is_folder)
            else:
                logging.info("Found " + name + " data")
        elif code == "api":
            if not os.path.exists("data_access/" + product['api_access_script'] + ".py"):
                logging.warning(
                    "Downloading " + name + "api access script. Why didn't you have this? Definitely reclone the "
                                            "project from github if you can")
                download_data_from_dropbox(product['api_access_script'], filename, is_folder)
            else:
                logging.info("Found " + name + " api access script")
    logging.info("Finished downloading required data & files")
    return


if __name__ == "__main__":
    print("This is the download data module. It downloads all the general local data that can't really be accessed "
          "through API's. These are massive files (4-5 GB)")
