from deltacat.utils.common import sha1_digest, sha1_hexdigest


class Locator:
    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        raise NotImplementedError()

    def digest(self) -> bytes:
        """
        Return a digest of the given locator that can be used for
        equality checks (i.e. two locators are equal if they have the
        same digest) and uniform random hash distribution.
        """
        return sha1_digest(self.canonical_string().encode("utf-8"))

    def hexdigest(self) -> str:
        """
        Returns a hexdigest of the given locator suitable
        for use in equality (i.e. two locators are equal if they have the same
        hexdigest) and inclusion in URLs.
        """
        return sha1_hexdigest(self.canonical_string().encode("utf-8"))

    def path(self, root: str, separator: str = "/") -> str:
        """
        Returns a path for the locator of the form: "{root}/{hexdigest}", where
        the default path separator of "/" may optionally be overridden with
        any string.
        """
        return f"{root}{separator}{self.hexdigest()}"
