from pathlib import Path

import pandas as pd


def getPrimes(limit: int = 200):
    primes_dir = Path(__file__).parent.joinpath(r'data\primes')

    files = [
        f"data/unzipped/2T_part{i}.txt" for i in range(1, limit + 1) if Path(f"data/unzipped/2T_part{i}.txt").exists()
    ]

    primes = list()

    for file in files:
        print(f"Reading {file}")

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

    return primes


if __name__ == '__main__':
    outfile = 'data/primes/primes.ftr'

    _primes = getPrimes(limit=200)

    # Create DataFrame
    _df = pd.DataFrame(_primes, columns=['primes'])

    print(f"\nWriting {_df.shape[0]} primes to {outfile}")

    # Write to feather file
    _df.to_feather(outfile)

    print(f"\nFinished writing to {outfile}. Feather file size: {round(Path(outfile).stat().st_size / 1024**2, 2)} MB")
