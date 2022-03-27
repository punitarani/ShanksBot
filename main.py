import pandas as pd
import multiprocessing as mp
from functools import partial

from divide import get_reciprocal


def find_reciprocals(primes: list) -> pd.DataFrame:
    """
    Finds the reciprocals of the primes in the list.

    :param primes: list of primes
    :return: DataFrame of reciprocals (prime: length of reciprocal, reciprocal)
    """

    # Find reciprocals in parallel
    with mp.Pool(mp.cpu_count()) as pool:
        outputs = pool.map(_find_reciprocal, primes)

    # Create DataFrame from outputs
    df = pd.DataFrame(outputs)
    df.set_index('prime', inplace=True)

    # Set datatypes
    df['length'] = df['length'].astype(int)
    df['reciprocal'] = df['reciprocal'].astype(float)

    return df


def _find_reciprocal(_prime: int) -> dict:
    """
    Finds the reciprocal of a prime.

    :param _prime: prime to find reciprocal of.
    :return: dict({prime, length of reciprocal, reciprocal})
    """
    reciprocal = get_reciprocal(_prime)

    length = int(len(reciprocal))
    reciprocal = float("0." + "".join(str(x) for x in reciprocal))

    # Log progress
    print(f"{_prime} -> {reciprocal}")

    return {'prime': _prime, 'length': length, 'reciprocal': reciprocal}


if __name__ == '__main__':
    infile = 'data/primes/thousand_primes.csv'
    outfile = 'data/reciprocals/thousand_reciprocals.csv'

    # Read in primes
    _primes = pd.read_csv(infile, index_col=0).index.tolist()

    # Find reciprocals
    _df = find_reciprocals(_primes)

    # Save df
    _df.to_csv(outfile)
    print(f"Saved to {outfile}")
