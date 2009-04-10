import math
import random

class DiscreteRandomVariable(object):
	""" Simulates a Discrete Random Variable """
	def __init__(self, values):
		""" <values>: a list of (v, p) pairs where p is the relative
		    probability for the value v """
		self.dist = list() # the scaled distribution of the variable
		c = 0.0
		for v, p in values:
			self.dist.append((v, c))
			c += p
		self.ub = c # the upperbound of self.dist
		# we'll use binary search, which is simpler with a list of
		# length 2**n -- thus we'll fake the cneter of the list:
		self.vc = 2**int(math.ceil(math.log(len(self.dist),2)))/2
	
	def pick(self):
		""" picks a value accoriding to the given density """
		v = random.uniform(0, self.ub)
		d = self.dist
		c = self.vc-1
		s = self.vc
		while True:
			s = s / 2
			if s == 0:
				break
			if v <= d[c][1]:
				c -= s
			else:
				c += s
				# we only need this logic when increasing c
				while len(d) <= c:
					s = s / 2
					c -= s
					if s == 0:
						break
		# we may have converged from the left, instead of the right
		if v <= d[c][1]:
			c -= 1
		return d[c][0]
