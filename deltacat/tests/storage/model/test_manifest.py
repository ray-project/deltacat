import json

import pytest

from deltacat.storage.model.manifest import Manifest, ManifestEntry


@pytest.fixture
def manifest_a():
    return """
        {
          "entries":[
            {
              "uri":"s3://test_bucket/file1.tsv.gz",
              "mandatory":true,
              "meta":{
                "record_count":0,
                "content_length":123,
                "source_content_length":0,
                "content_type":"application/x-amzn-unescaped-tsv",
                "content_encoding":"gzip"
              }
            },
            {
              "uri":"s3://test_bucket/file2.tsv.gz",
              "mandatory":true,
              "meta":{
                "record_count":0,
                "content_length":456,
                "source_content_length":0,
                "content_type":"application/x-amzn-unescaped-tsv",
                "content_encoding":"gzip"
              }
            }
          ],
          "meta":{
            "record_count":0,
            "content_length":579,
            "source_content_length":0,
            "content_type":"application/x-amzn-unescaped-tsv",
            "content_encoding":"gzip"
          },
          "id":"052f62c0-5082-4935-9937-18a705156123",
          "author":{
            "name":"Dave",
            "version":"1.0"
          }
        }
           """


@pytest.fixture
def manifest_no_author():
    return """
        {
          "entries":[
            {
              "uri":"s3://test_bucket/file1.tsv.gz",
              "mandatory":true,
              "meta":{
                "record_count":0,
                "content_length":123,
                "source_content_length":0,
                "content_type":"application/x-amzn-unescaped-tsv",
                "content_encoding":"gzip"
              }
            },
            {
              "uri":"s3://test_bucket/file2.tsv.gz",
              "mandatory":true,
              "meta":{
                "record_count":0,
                "content_length":456,
                "source_content_length":0,
                "content_type":"application/x-amzn-unescaped-tsv",
                "content_encoding":"gzip"
              }
            }
          ],
          "meta":{
            "record_count":0,
            "content_length":579,
            "source_content_length":0,
            "content_type":"application/x-amzn-unescaped-tsv",
            "content_encoding":"gzip"
          },
          "id":"052f62c0-5082-4935-9937-18a705156123"
        }
           """


@pytest.fixture()
def manifest_entry_no_meta():
    return """
            {
              "uri":"s3://test_bucket/file1.tsv.gz",
              "mandatory":true
            }
           """


def test_manifest_from_json(manifest_a):
    manifest = Manifest.from_json(manifest_a)

    assert manifest.entries is not None
    assert len(manifest.entries) == 2
    assert manifest.entries[0].uri == "s3://test_bucket/file1.tsv.gz"
    assert manifest.entries[0].meta.record_count == 0
    assert manifest.meta.content_length == 579
    assert manifest.author.name == "Dave"


def test_manifest_from_json_no_author(manifest_no_author):
    manifest = Manifest.from_json(manifest_no_author)

    assert manifest.entries is not None
    assert len(manifest.entries) == 2
    assert manifest.entries[0].uri == "s3://test_bucket/file1.tsv.gz"
    assert manifest.entries[0].meta is not None
    assert manifest.author is None


def test_manifest_entry_from_dict_no_meta(manifest_entry_no_meta):
    entry = ManifestEntry.from_dict(json.loads(manifest_entry_no_meta))

    assert entry is not None
    assert entry.meta is None
    assert entry.uri == "s3://test_bucket/file1.tsv.gz"
    assert entry.mandatory is True
