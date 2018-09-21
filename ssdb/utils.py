def from_url(url, **kwargs):
    """
    Returns an active SSDB client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from ssdb.client import SSDB
    return SSDB.from_url(url, **kwargs)


class dummy(object):
    """
    Instances of this class can be used as an attribute container.
    """
    pass
