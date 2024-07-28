import hashlib


def calculate_checksum(data: bytes | str) -> str:
    """
    Calculates the SHA-256 checksum of the given data.

    :param data: The data to calculate the checksum for.
    :return: The checksum of the data in hex representation.
    """
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()


def remove_if_exists(data_store: dict, key_name: str) -> dict:
    """
    Removes an item from a dictionary if it exists.

    :param data_store: The dictionary to remove the item from.
    :param key_name: The key of the item to remove from the dictionary.
    :return: The original dictionary with the item removed if existed.
    """
    if key_name in data_store.keys():
        del data_store[key_name]
    return data_store
