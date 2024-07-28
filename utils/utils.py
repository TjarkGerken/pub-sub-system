import hashlib


def calculate_checksum(data: bytes or str) -> str:
    """
    Calculates the SHA-256 checksum of the given data.

    :return:
    :param data: The data to calculate the checksum for.
    :type data: str
    :return: The checksum of the data in hex representation.
    """
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()
