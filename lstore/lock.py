import threading 

class readWriteLock:

	## a record could have a list of shared locks but a single write lock
	def __init__(self):
		self._read_ready = threading.Lock()
		self._reader = 0

	## acquire a read lock 
	def acquire_read(self):
		if self._read_ready.acquire(blocking=False):
			try:
				self._reader += 1
			finally:
				self._read_ready.release()
			return True
		else:
			# print('too late')
			return False

	## release a read lock 
	def release_read(self):
		self._read_ready.acquire()
		try:
			self._reader -= 1
		finally:
			self._read_ready.release()

	## acquire a write lock only when there is no shared lock on the record
	def acquire_write(self):
		if self._read_ready.acquire(blocking=False):
			if bool(self._reader):
				self._read_ready.release()
				return False
			else:
				return True
		else:
			return False

	## release the write lock
	def release_write(self):
		self._read_ready.release()


if __name__ == '__main__':

	rw1 = readWriteLock()
	rw2 = readWriteLock()
	print(rw1.acquire_read())
	print(rw1.release_read())
	print(rw1.acquire_write())
	print(rw1.acquire_write())
	print("-----------------------")
	print(rw2.acquire_write())
	print(rw2.acquire_write())


