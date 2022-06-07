from typing import List, Iterable


class PartitionKeyValue(dict):
    @staticmethod
    def of(key_name: str,
           key_type: str,
           value: str):
        pkv = PartitionKeyValue()
        pkv["key"] = {}
        pkv["key"]["keyName"] = key_name
        pkv["key"]["keyType"] = key_type
        pkv["value"] = value
        return pkv

    @property
    def key(self):
        return self["key"]

    @property
    def key_name(self):
        return self["key"]["keyName"]

    @property
    def key_type(self):
        return self["key"]["keyType"]

    @property
    def value(self):
        return self["value"]


class PartitionKeyValues(tuple):
    @property
    def id(self):
        return ".".join([pkv.value for pkv in self if isinstance(pkv, PartitionKeyValue)])
