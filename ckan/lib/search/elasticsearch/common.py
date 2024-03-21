# encoding: utf-8
from __future__ import annotations

import datetime
import logging
import re
from typing import Any, Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from ckan.common import config

log = logging.getLogger(__name__)

class SearchIndexError(Exception):
    pass

class SearchError(Exception):
    pass

class SearchQueryError(SearchError):
    pass

DEFAULT_ELASTICSEARCH_URL = 'http://127.0.0.1:9200'

class SearchEngineSettings(object):
    _is_initialised: bool = False
    _url: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None

    @classmethod
    def init(cls,
             url: Optional[str] = None,
             user: Optional[str] = None,
             password: Optional[str] = None) -> None:
        if url is not None:
            cls._url = url
            cls._user = user
            cls._password = password
        else:
            cls._url = DEFAULT_ELASTICSEARCH_URL
        cls._is_initialised = True

    @classmethod
    def get(cls) -> tuple[str, Optional[str], Optional[str]]:
        if not cls._is_initialised:
            raise SearchIndexError('Elasticsearch URL not initialised')
        if not cls._url:
            raise SearchIndexError('Elasticsearch URL is blank')
        return (cls._url, cls._user, cls._password)

def is_available() -> bool:
    """
    Return true if we can successfully connect to Elasticsearch.
    """
    try:
        conn = make_connection()
        conn.info()
    except Exception as e:
        log.exception(e)
        return False
    return True

def make_connection() -> Elasticsearch:
    es_url, es_user, es_password = SearchEngineSettings.get()

    if es_url and es_user and es_password:
        es = Elasticsearch(
            [es_url],
            http_auth=(es_user, es_password)
        )
    else:
        es = Elasticsearch(
            [es_url]
        )

    timeout = config.get('elasticsearch_timeout', 10)
    es.transport.connection_pool.connection_timeout = timeout
    return es

def elasticsearch_datetime_decoder(d: dict[str, Any]) -> dict[str, Any]:
    for k, v in d.items():
        if isinstance(v, str):
            try:
                d[k] = datetime.datetime.fromisoformat(v)
            except ValueError:
                pass  # Not a datetime string, ignore
    return d
