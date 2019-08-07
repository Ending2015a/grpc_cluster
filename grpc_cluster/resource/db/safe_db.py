import os
import sys
import time
import logging

from tinydb import TinyDB, Query
from tinydb import Table as tinyTable
from tinyrecord import transaction

class DB(TinyDB):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def insert(self, row):
        with transaction(self) as tr:
            return tr.insert(row)

    def update(self, fields, query=null_query, doc_ids=[], eids=[]):
        with transaction(self) as tf:
            return tr.update(fields, query, doc_ids, eids)
    

    def table(self, name='_default', **options):
        return super().table(name, **options)


class Table(tinyTable):
    def __init__(self, storage, name, cache_size=10):
        super().__init__(storage, name, cache_size)

    def insert(self, document):
        with transaction(self) as tr:
            return tr.insert(document)

    def update(self, fields, cond=None, doc_ids=None, eids=None):
        with transaction(self) as tr:
            return tr.update(fields, cond, doc_ids, eids)


