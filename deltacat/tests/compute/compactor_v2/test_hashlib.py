import hashlib


def test_hashlib_sanity():
    """
    This test ensures that there is no change in hashlib behavior
    across different python version. If there is, a rebase is required.
    """
    assert (
        hashlib.sha1("test".encode("utf-8")).hexdigest()
        == "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
    )
