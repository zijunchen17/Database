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
                return self.abort()
            # Successfully acquired lock. Note.
            else:
                #TODO: Make cleaner way to do this using .__self__
                method_name = query.__name__
                if method_name in self.ROLLBACK_METHODS: # lol
                    rid = query.__self__.table.key_directory[args[0]]
                    lock = query.__self__.table.lock_manager[rid]
                    self.write_query_locks.append(lock)
        return self.commit()

    def abort(self):
    
        for lock in self.write_query_locks:
            lock.release_write()
        #TODO: do roll-back and any other necessary operations        
        return False

    def commit(self):
        map(lambda obj: obj.write_query_locks, self.write_query_locks)
        # same as loop in abort

        # TODO: commit to database
        return True

