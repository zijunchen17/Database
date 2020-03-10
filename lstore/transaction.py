from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.writing_queries = []
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
            

            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # Successfully acquired lock. Note.
            else:
                q_str = str(query)
                q_str = q_str.split(' ')
                q_str = q_str[2]
                q_str = q_str.split('.')
                q_str = q_str[-1]
                if q_str in ["select", "increment", "update"]: # lol
                    print("matched")
                    self.writing_queries.append(query)
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        
        return False

    def commit(self):
        # TODO: commit to database
        return True

