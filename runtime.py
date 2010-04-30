import functools

class ExceptionCatchingWrapper(object):
	def __init__(self, wrapped, callback):
		""" Creates a wrapper around <wrapped>.  If any call to a
		    callable attribute throws an exception, callback will be
		    called with the name of the callable attribute as first
		    argument and the exception as second. The return value
		    of the callback will be the return value of the callable
		    attribute. """
		object.__setattr__(self, '_wrapped', wrapped)
		object.__setattr__(self, '_callback', callback)
	def __getattribute__(self, name):
		wrapped = object.__getattribute__(self, '_wrapped')
		a = getattr(wrapped, name)
		if callable(a):
			cb = object.__getattribute__(self, '_callback')
			def wrapper(*args, **kwargs):
				try:
					return a(*args, **kwargs)
				except Exception as e:
					return cb(name, e)
			return wrapper
		return a
	def __setattr__(self, name, value):
		wrapped = object.__getattribute__(self, '_wrapped')
		setattr(wrapped, name, value)
	def __delattr__(self, name):
		wrapped = object.__getattribute__(self, '_wrapped')
		delattr(wrapped, name)

