# encoding: utf-8
from __future__ import annotations

import logging
import json
from typing import Any, Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from ckan.common import config

from ckan.lib.search.common import SearchIndexError, make_connection
import ckan.model as model
from ckan.plugins import PluginImplementations, IPackageController
import ckan.logic as logic
import ckan.lib.plugins as lib_plugins

log = logging.getLogger(__name__)

TYPE_FIELD = "entity_type"
PACKAGE_TYPE = "package"

class SearchIndex(object):
    def __init__(self) -> None:
        self.es = make_connection()

    def insert_dict(self, data: dict[str, Any]) -> None:
        return self.update_dict(data)

    def update_dict(self, data: dict[str, Any], defer_commit: bool = False) -> None:
        log.debug("NOOP Index: %s" % ",".join(data.keys()))

    def remove_dict(self, data: dict[str, Any]) -> None:
        log.debug("NOOP Delete: %s" % ",".join(data.keys()))

    def clear(self) -> None:
        # Clearing the index in Elasticsearch can be done by deleting and recreating the index
        index_name = config.get('ckan.elasticsearch.index', 'ckan')
        self.es.indices.delete(index=index_name, ignore=[400, 404])
        self.es.indices.create(index=index_name)

class NoopSearchIndex(SearchIndex): pass

class PackageSearchIndex(SearchIndex):
    def preprocess_package_dict_for_elasticsearch(pkg_dict):
        """
        Pre-processes a CKAN package dict for indexing in Elasticsearch.

        :param pkg_dict: The original package dict from CKAN.
        :return: A transformed package dict ready for Elasticsearch indexing.
        """

        # Clone the original dictionary to avoid modifying it directly
        es_pkg_dict = pkg_dict.copy()

        # Elasticsearch prefers dates in ISO8601 format
        for date_field in ['metadata_created', 'metadata_modified']:
            if date_field in es_pkg_dict and isinstance(es_pkg_dict[date_field], datetime.datetime):
                es_pkg_dict[date_field] = es_pkg_dict[date_field].isoformat()

        fields_to_remove = ['state', 'revision_id']
        for field in fields_to_remove:
            es_pkg_dict.pop(field, None)

        # Transform lists or arrays into a format suitable for Elasticsearch
        if 'tags' in es_pkg_dict:
            es_pkg_dict['tags'] = [tag['name'] for tag in es_pkg_dict['tags']]

        # Handling nested objects or complex structures
        if 'organization' in es_pkg_dict:
            es_pkg_dict['organization'] = es_pkg_dict['organization']['name']

        # Custom transformations or additions
        if 'extras' in es_pkg_dict:
            extras_text = ' '.join([f"{extra['key']}: {extra['value']}" for extra in es_pkg_dict['extras']])
            es_pkg_dict['extras_search_text'] = extras_text

        return es_pkg_dict

    # Usage example
    
# pkg_dict = {
#     'id': 'example-dataset-1',
#     'name': 'Example Dataset',
#     'tags': [{'name': 'Open Data'}, {'name': 'Public'}],
#     'metadata_created': datetime.datetime.now(),
#     'metadata_modified': datetime.datetime.now(),
#     'state': 'active',
#     'revision_id': 'rev-1234',
#     'extras': [{'key': 'source', 'value': 'Survey'}]
# }

# es_pkg_dict = preprocess_package_dict_for_elasticsearch(pkg_dict)


    def index_package(self, pkg_dict: Optional[dict[str, Any]], defer_commit: bool = False) -> None:
        if pkg_dict is None:
            return

        # Placeholder for pre-processing the package dict as needed for Elasticsearch
        # when new schema is applied

        try:
            self.es.index(index=config.get('ckan.elasticsearch.index', 'ckan'),
                          id=pkg_dict['id'],
                          document=pkg_dict)
            if not defer_commit:
                self.es.indices.refresh(index=config.get('ckan.elasticsearch.index', 'ckan'))
        except Exception as e:
            log.exception(e)
            raise SearchIndexError(f"Error indexing package: {e}")

    def update_dict(self, pkg_dict: dict[str, Any], defer_commit: bool = False) -> None:
        self.index_package(pkg_dict, defer_commit)

    def remove_dict(self, pkg_dict: dict[str, Any]) -> None:
        try:
            self.es.delete(index=config.get('ckan.elasticsearch.index', 'ckan'),
                           id=pkg_dict['id'])
            if not defer_commit:
                self.es.indices.refresh(index=config.get('ckan.elasticsearch.index', 'ckan'))
        except NotFoundError:
            log.info(f"Package {pkg_dict['id']} not found in Elasticsearch index.")
        except Exception as e:
            log.exception(e)
            raise SearchIndexError(f"Error removing package: {e}")

    def commit(self) -> None:
        # Elasticsearch automatically handles commit operations, so this might not be necessary
        pass

