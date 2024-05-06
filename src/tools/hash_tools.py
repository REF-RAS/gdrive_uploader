# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

# Reference: https://stackoverflow.com/posts/56915300/revisions
import os, hashlib, hmac

def hash_new_password(password: str):
    """ Hash the provided password with a randomly-generated salt and return the salt and hash to store in the database.

    """
    salt = os.urandom(16)
    pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    return salt, pw_hash

def is_correct_password(salt: bytes, pw_hash: bytes, password: str) -> bool:
    """ Given a previously-stored salt and hash, and a password provided by a user trying to log in, check whether the password is correct.

    """
    return hmac.compare_digest(
        pw_hash,
        hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
    )
    
def hash_bytearray(content:bytes) -> str:
    """ Returns the md5 hash as a hex string of the given bytearray

    :param content: The bytearray of which the md5 hash is to be computed
    :return: The md5 hash as a hex string
    """
    return hashlib.md5(content).hexdigest()

# the main test program
if __name__ == '__main__':
    # Example usage:
    salt, pw_hash = hash_new_password('correct horse battery staple')
    assert is_correct_password(salt, pw_hash, 'correct horse battery staple')
    assert not is_correct_password(salt, pw_hash, 'Tr0ub4dor&3')
    assert not is_correct_password(salt, pw_hash, 'rosebud')