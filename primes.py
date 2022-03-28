import multiprocessing as mp
import pickle
from functools import partial
from pathlib import Path


def getPrimes(limit: int = 200):
    """
    Read all files and return a list of primes

    :param limit: The maximum number of files to read
    :return: A list of primes
    """

    pickle_file = r'data/primes/primes.pkl'

    if Path(pickle_file).exists():
        with open(pickle_file, 'rb') as f:
            print(f"Reading {pickle_file}")
            primes = pickle.load(f)

    else:
        files = [
            f"data/unzipped/2T_part{i}.txt" for i in range(1, limit + 1) if Path(f"data/unzipped/2T_part{i}.txt").exists()
        ]

        primes = list()

        cores = mp.cpu_count() - 1 if mp.cpu_count() > 1 else 1

        with mp.Pool(cores) as pool:
            results = pool.map(readFile, files)

        for result in results:
            primes.extend(result)

    return primes


def readFile(file: str):
    """
    Reads a file and returns a list primes

    :param file: The file to read
    :return: A list of primes
    """

    primes = list()

    print(f"Reading {file}")

    try:
        with open(file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                # Remove newline character
                line = line.replace('\n', '')

                # Remove tab character
                line = line.replace('\t', ',')

                # Check for blank lines
                if line != '':
                    line = line.strip()
                    line = line.split(',')

                    for prime in line:
                        if prime != '':
                            primes.append(int(prime))
    except Exception as e:
        print(f"Error {e} while reading file {file}")

    return primes


def convertFiles(limit: int = 200):
    """
    Read all files and write to feather file

    :param limit: The maximum number of files to read
    """

    files = [
        f"data/unzipped/2T_part{i}.txt" for i in range(1, limit + 1) if Path(f"data/unzipped/2T_part{i}.txt").exists()
    ]

    outfile = r"data/primes/primes.pkl"

    cores = mp.cpu_count() - 1 if mp.cpu_count() > 1 else 1

    with mp.Pool(cores) as pool:
        poolFunc = partial(convertFile, outfile=outfile)
        primes = pool.map(poolFunc, files)

    print(
        f"\nFinished writing to {outfile}. Feather file size: {round(Path(outfile).stat().st_size / 1024 ** 2, 2)} MB")

    return primes


def convertFile(infile: str, outfile: str):
    """
    Read primes from txt file and write to feather file

    :param infile: The file to read from
    :param outfile: The file to write to
    """

    primes = readFile(infile)

    if Path(outfile).exists():
        print(f"Appending {infile} to {outfile}")

        # Append list to existing pickle file
        with open(outfile, 'rb') as f:
            data = pickle.load(f)

        data.extend(primes)

        with open(outfile, 'wb') as f:
            pickle.dump(data, f)

    else:
        print(f"\nCreating {outfile}")
        print(f"Writing {infile} to {outfile}")

        # Create new pickle file
        with open(outfile, 'wb') as f:
            pickle.dump(primes, f)

    return primes


if __name__ == '__main__':
    convertFiles(200)
