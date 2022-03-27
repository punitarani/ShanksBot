# Download prime numbers files

import multiprocessing as mp
import sys
from pathlib import Path

import py7zr
import wget


def downloadFiles(limit: int = 200) -> None:
    """
    Download files from url to zips folder

    :param limit: Limit of files to download
    """

    def getURL(n):
        return f'http://www.primos.mat.br/dados/2T_part{n}.7z'

    urls = [getURL(n) for n in range(1, limit + 1)]

    print("\nDownloading files...")

    # Download files in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(downloadFile, urls)


def downloadFile(url: str) -> None:
    """
    Download file from url to zips folder

    :param url: URL to download file from
    :return: None
    """

    zips_dir = r'data/zips'

    filename = url.split('/')[-1]

    # Check if file already exists
    if not Path(zips_dir).joinpath(filename).exists():
        wget.download(url, out=zips_dir, bar=bar_progress)
        print(f"Downloaded {url}")

    else:
        print(f"File {filename} already exists")


def bar_progress(current, total, width=80):
    """
    create this bar_progress method which is invoked automatically from wget
    """

    progress_message = "Downloading: %d%% [%d / %d] bytes. " % (current / total * 100, current, total)

    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()


def unzipFiles(limit: int = 200) -> None:
    """
    Unzip files in zips folder

    :param limit: Limit of files to unzip
    """

    files = [f"data/zips/2T_part{n}.7z" for n in range(1, limit + 1)]

    print("\nUnzipping files...")

    # Check if file already exists
    for file in files:
        if Path(file).exists():
            unzip(file)


def unzip(filepath) -> None:
    """
    Unzip file

    :param filepath: Path to file to unzip
    """

    unzipped_dir = r'data/unzipped'

    # Check if file already exists
    try:
        with py7zr.SevenZipFile(filepath, mode='r') as z:
            z.extractall(unzipped_dir)
            print(f"Unzipped {filepath}")

    except FileNotFoundError:
        print(f"File {filepath} does not exist")


def mkDirs() -> None:
    """
    Create directories for data
    """

    print("\nCreating necessary directories...")

    for _dir in ['data', 'data/zips', 'data/unzipped']:
        if not Path(_dir).exists():
            Path(_dir).mkdir(parents=True)
            print(f"Created {_dir}")


if __name__ == '__main__':
    files_limit = 200

    mkDirs()
    downloadFiles(files_limit)
    unzipFiles(files_limit)
