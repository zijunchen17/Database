from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
import threading
class Transaction:

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
        for query, args in self.queries:
            method_name = query.__name__
            if method_name in self.ROLLBACK_METHODS: # lol    
                rid = query.__self__.table.key_directory[args[0]]
                base_schema = query.__self__.table.get_base_schema(rid)
                lock = query.__self__.table.lock_manager[rid]

            result = query(*args)

            if method_name in self.ROLLBACK_METHODS and result != False:
                self.write_methods.append(method_name)
                self.write_rids.append(rid)
                self.write_original_schemas.append(base_schema)
                self.write_query_locks.append(lock)

            # If the query has failed the transaction should abort
            # if result == False:
            if result == False and method_name in self.ROLLBACK_METHODS:  # If query couldn't acquire key
                return self.abort(query.__self__.table)
            # Successfully acquired lock. Note.
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

