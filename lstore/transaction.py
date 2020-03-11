from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
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
        self.ROLLBACK_METHODS = ["insert", "increment", "update"]
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            # print("args: ",args)
            result = query(*args)
            
            # If the query has failed the transaction should abort
            if result == False:  # If query couldn't acquire key
                print("query aborted")
                return self.abort(query.__self__.table)
            # Successfully acquired lock. Note.
            else:
                #TODO: Make cleaner way to do this using .__self__
                method_name = query.__name__
                if method_name in self.ROLLBACK_METHODS: # lol
                    rid = query.__self__.table.key_directory[args[0]]
                    lock = query.__self__.table.lock_manager[rid]
                    base_schema = query.__self__.table.get_base_schema(rid)
                    self.write_rids.append(rid)
                    self.write_query_locks.append(lock)
                    self.write_original_schemas.append(base_schema)
                    self.write_methods.append(method_name)

        return self.commit()

    def abort(self, table):
        print("ABORT")
        for base_rid, original_schema, method_name in zip(self.write_rids, self.write_original_schemas, self.write_methods):
            table.rollback(base_rid, original_schema)

        for lock in self.write_query_locks:
            lock.release_write()
            
        #TODO: do roll-back and any other necessary operations        
        return False

    def commit(self):
        for lock in self.write_query_locks:
            lock.release_write()
        # same as loop in abort

        # TODO: commit to database
        return True

