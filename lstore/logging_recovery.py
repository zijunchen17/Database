import logging
from threading import Lock

class logging_recovery:

    def __init__(self):
        self.logger = logging.getLogger('transaction history')
        logging.basicConfig(filename = 'logging_history.log', level=logging.INFO)
        self.f_handler = logging.FileHandler('logging_history.log')
        # self.f_format = logging.Formatter('%(asctime)s - %(message)s')
        self.f_format = logging.Formatter('%(message)s')
        self.f_handler.setFormatter(self.f_format)
        self.logger.addHandler(self.f_handler)
        self.logger_lock = Lock()

    def transaction_start(self, transaction_id):
        with self.logger_lock:
            self.logger.info(f'transaction_id:{transaction_id}, Start')

    def transaction_change(self, transaction_id, query_type, args, old_value):
        if query_type == "increment":
            with self.logger_lock:
                self.logger.info(f'transaction_id:{transaction_id}, query_type:{query_type}, key:{args[0]}, '
                                 f'old value:{old_value.columns[args[1]]}, update_column: {args[1]}')

    def transaction_abort(self, transaction_id):
        with self.logger_lock:
            self.logger.info(f'transaction_id:{transaction_id}, Aborted')

    def transaction_commit(self, transaction_id):
        with self.logger_lock:
            self.logger.info(f'transaction_id:{transaction_id}, Committed')