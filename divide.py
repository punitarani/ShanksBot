
def get_reciprocal(n: int) -> list:
    """
    Returns the reciprocal of a number.

    :param n: The number to find the reciprocal of.
    :return: First n-non-repeating digits reciprocal.
    """

    # quotient = [0, 0, 1, 2, 3, 4] = 0.001234
    quotient: list = list()
    remainders: list = list()

    dividend = 1
    repeats: bool = False

    while not repeats:
        # Carry over
        if n > dividend:
            quotient.append(0)
            dividend = dividend * 10
            remainders.append(dividend)

        # Divide
        if dividend % n == 0:
            quotient.append(dividend // n)
            repeats = True

        # Divide and calculate remainder
        elif n < dividend:
            quotient.append(dividend // n)
            dividend = dividend % n * 10

            if dividend in remainders:
                repeats = True

            remainders.append(dividend)

    # Remove first 0
    quotient.pop(0)

    return quotient
