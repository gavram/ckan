# encoding: utf-8
from __future__ import annotations

import logging
from typing import Any, Optional, Dict

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

from ckan.common import config
from ckan.lib.search.common import make_connection

log = logging.getLogger(__name__)

def make_connection() -> Elasticsearch:
    es_url = config.get('ckan.elasticsearch.url', 'http://localhost:9200')
    es = Elasticsearch([es_url])
    return es

class SearchQuery(object):
    def __init__(self) -> None:
        self.results = []
        self.count = 0
        self.facets = {}

    def run(self, query: dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        raise NotImplementedError("SearchQuery.run() must be implemented by subclasses.")

class PackageSearchQuery(SearchQuery):
    def run(self, query: dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        es = make_connection()
        index_name = config.get('ckan.elasticsearch.index', 'ckan')
        s = Search(using=es, index=index_name)

        # Construct the query
        if 'q' in query and query['q']:
            q = Q("multi_match", query=query['q'], fields=["name^4", "title^4", "tags^2", "groups^2", "text"])
        else:
            q = Q("match_all")

        # Apply filters
        if 'fq' in query:
            for fq in query['fq']:
                s = s.filter('term', **fq)

        # Sorting
        if 'sort' in query:
            s = s.sort(query['sort'])

        # Pagination
        if 'start' in query:
            s = s.extra(from_=query['start'])
        if 'rows' in query:
            s = s.extra(size=query['rows'])

        # Execute the query
        response = s.query(q).execute()

        # Process the results
        self.results = [hit.to_dict() for hit in response.hits]
        self.count = response.hits.total.value

        # Handle facets if requested
        if 'facet' in query and query['facet']:
            self.facets = self._process_facets(s, response)

        return {'results': self.results, 'count': self.count, 'facets': self.facets}

    def _process_facets(self, search: Search, response: Any) -> Dict[str, Any]:
        # Placeholder for facet processing
        return {}

# Testing usage
if __name__ == "__main__":
    query = {
        'q': 'open data',
        'fq': [{'state': 'active'}],
        'sort': 'name asc',
        'start': 0,
        'rows': 10,
        'facet': True
    }

    search_query = PackageSearchQuery()
    results = search_query.run(query)
    print(results)
