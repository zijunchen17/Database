from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
import itertools

import threading
class Transaction:
    # to generate transaction id
    id_iter = itertools.count()

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.write_query_locks = []
        
        self.write_rids = []
        self.write_original_schemas = []
        self.write_methods = []

        # self.ROLLBACK_METHODS = ["insert", "increment", "update"]
        self.ROLLBACK_METHODS = ["increment", "update"]
        self.num_aborts = 0
        self.abort_lock = threading.Lock()

        self.transaction_id = next(self.id_iter)
        pass

    def add_query(self, query, *args):
        """
        # Adds the given query to this transaction
        # Example:
        # q = Query(grades_table)
        # t = Transaction()
        # t.add_query(q.update, 0, *[None, 1, None, 2, None])
        """
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        start_temp  = 0
        old_value = []

        for query, args in self.queries:
            if start_temp == 0:
                query.__self__.table.logging_history.transaction_start(self.transaction_id)
                start_temp += 1
            else:
                pass

            method_name = query.__name__
            if method_name == 'select':
                result = query(*args)
                if result == False:
                    query.__self__.table.logging_history.transaction_abort(self.transaction_id)
                    return self.abort(query.__self__.table)
                old_value.append(result)

            if method_name in self.ROLLBACK_METHODS: # lol
                transaction_id = self.transaction_id
                original_value = old_value[-1][0]
                query.__self__.table.logging_history.transaction_change(transaction_id, method_name, args, original_value)
                rid = query.__self__.table.key_directory[args[0]]
                base_schema = query.__self__.table.get_base_schema(rid)
                lock = query.__self__.table.lock_manager[rid]
                result = query(*args)

            # result = query(*args)

            if method_name in self.ROLLBACK_METHODS and result != False:
                self.write_methods.append(method_name)
                self.write_rids.append(rid)
                self.write_original_schemas.append(base_schema)
                self.write_query_locks.append(lock)

            # If the query has failed the transaction should abort
            # if result == False:
            if result == False and method_name in self.ROLLBACK_METHODS:  # If query couldn't acquire key
                query.__self__.table.logging_history.transaction_abort(self.transaction_id)
                return self.abort(query.__self__.table)
            # Successfully acquired lock. Note.
        query.__self__.table.logging_history.transaction_abort(self.transaction_id)
        return self.commit()

    def abort(self, table):
        self.abort_lock.acquire()
        self.num_aborts += 1
        self.abort_lock.release()
        # print("base_rids", self.write_rids)
        # print("methods", self.write_methods)
        if self.write_methods:
            for base_rid, original_schema, method_name in zip(self.write_rids, self.write_original_schemas, self.write_methods):
                table.rollback(base_rid, original_schema, method_name)

        for lock in self.write_query_locks:
            lock.release_write()
        return False

    def commit(self):
        for lock in self.write_query_locks:
            lock.release_write()
        return True

