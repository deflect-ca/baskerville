import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import (
    Cipher, algorithms, modes
)
import hashlib


def encrypt(key, plaintext, associated_data):
    """

    :param key:
    :param plaintext:
    :param associated_data:
    :return:
    """
    # Generate a random 96-bit IV.
    iv = os.urandom(12)

    # Construct an AES-GCM Cipher object with the given key and a
    # randomly generated IV.
    encryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(iv),
        backend=default_backend()
    ).encryptor()

    # associated_data will be authenticated but not encrypted,
    # it must also be passed in on decryption.
    encryptor.authenticate_additional_data(associated_data)

    # Encrypt the plaintext and get the associated ciphertext.
    # GCM does not require padding.
    ciphertext = encryptor.update(plaintext) + encryptor.finalize()

    return iv, ciphertext, encryptor.tag


def decrypt(key, associated_data, iv, ciphertext, tag):
    """

    :param key:
    :param associated_data:
    :param iv:
    :param ciphertext:
    :param tag:
    :return:
    """
    # Construct a Cipher object, with the key, iv, and additionally the
    # GCM tag used for authenticating the message.
    decryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(iv, tag),
        backend=default_backend()
    ).decryptor()

    # We put associated_data back in or the tag will fail to verify
    # when we finalize the decryptor.
    decryptor.authenticate_additional_data(associated_data)

    # Decryption gets us the authenticated plaintext.
    # If the tag does not match an InvalidTag exception will be raised.
    return decryptor.update(ciphertext) + decryptor.finalize()


#  todo: move to tests
def main():
    key = 'writeapassphrase'
    hashed_key = hashlib.sha256(key)
    other_digest = hashed_key.digest()

    iv, ciphertext, tag = encrypt(
        other_digest,
        b"a secret message!",
        b"authenticated but not encrypted payload"
    )

    encrypted_message = encrypt(
        other_digest,
        b"a secret message!",
        b"authenticated but not encrypted payload"
    )

    print(encrypted_message)

    decrypted_msg = decrypt(
        other_digest,
        b"authenticated but not encrypted payload",
        iv,
        ciphertext,
        tag
    )

    print(decrypted_msg)


if __name__ == "__main__":
    main()
