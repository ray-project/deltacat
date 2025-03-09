# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, List

from deltacat.utils.common import sha1_digest, sha1_hexdigest

DEFAULT_NAME_SEPARATOR = "|"
DEFAULT_PATH_SEPARATOR = "/"


class LocatorName:
    """
    Assigns a name to a catalog object. All sibling catalog objects must be
    assigned unique names (e.g., all namespaces in a catalog must be assigned
    unique locator names, all tables under a namespace must be assigned unique
    locator names, etc.). Names may be mutable (e.g., namespace and table names)
    or immutable (e.g., partition/stream IDs and delta stream positions). Names
    may be single or multi-part.
    """

    @property
    def immutable_id(self) -> Optional[str]:
        """
        If this locator name is immutable (i.e., if the object it refers to
        can't be renamed) then returns an immutable ID suitable for use in
        URLS or filesystem paths. Returns None if this locator name is mutable
        (i.e., if the object it refers to can be renamed).
        """
        raise NotImplementedError()

    @immutable_id.setter
    def immutable_id(self, immutable_id: Optional[str]) -> None:
        """
        If this locator name is immutable (i.e., if the object it refers to
        can't be renamed), then sets an immutable ID for this
        locator name suitable for use in URLS or filesystem paths. Note that
        the ID is only considered immutable in durable catalog storage, and
        remains mutable in transient memory (i.e., this setter remains
        functional regardless of whether an ID is already assigned, but each
        update causes it to refer to a new, distinct object in durable storage).
        """
        raise NotImplementedError()

    def parts(self) -> List[str]:
        """
        Returns the ordered parts of this locator's name.
        """
        raise NotImplementedError()

    def join(self, separator: str = DEFAULT_NAME_SEPARATOR) -> str:
        """
        Returns this locator name as a string by joining its parts with the
        given separator.
        """
        return separator.join(self.parts())

    def exists(self) -> bool:
        """
        Returns True if this locator name is defined, False otherwise.
        """
        return self.immutable_id or all(self.parts())


class Locator:
    """
    Creates a globally unique reference to any named catalog object. Locators
    are composed of the name of the referenced catalog object and its parent
    Locator (if any). Every Locator has a canonical string representation that
    can be used for global equality checks. Cryptographic digests of this
    canonical string can be used for uniform random hash distribution and
    path-based references to the underlying catalog object in filesystems or
    URLs.
    """

    @property
    def name(self) -> LocatorName:
        """
        Returns the name of this locator.
        """
        raise NotImplementedError()

    @property
    def parent(self) -> Optional[Locator]:
        """
        Returns the parent of this locator, if any.
        """
        raise NotImplementedError()

    def canonical_string(self, separator: str = DEFAULT_NAME_SEPARATOR) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        parts = []
        parent_hexdigest = self.parent.hexdigest() if self.parent else None
        if parent_hexdigest:
            parts.append(parent_hexdigest)
        parts.extend(self.name.parts())
        return separator.join([str(part) for part in parts])

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

    def path(self, root: str, separator: str = DEFAULT_PATH_SEPARATOR) -> str:
        """
        Returns a path for the locator of the form: "{root}/{hexdigest}", where
        the default path separator of "/" may optionally be overridden with
        any string.
        """
        return f"{root}{separator}{self.hexdigest()}"
