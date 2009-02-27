from __future__ import with_statement

import threading
import optparse
import logging
import os.path
import yaml
import os

def _get_by_path(bits, _globals):
	c = None
	for i, bit in enumerate(bits):
		try:
			c = globals()[bit] if c is None else getattr(c, bit)
		except (AttributeError, KeyError):
			c = __import__('.'.join(bits[:i+1]), _globals,
				fromlist=[bits[i+1]] if i+1 < len(bits) else [])
	return c

def get_by_path(path, _globals=None):
	""" Returns an object by <path>, importing modules if necessary """
	if _globals is None: _globals = list()
	return _get_by_path(path.split('.'), _globals)

def restricted_cover(l, succsOf):
	""" Returns a restricted <succsOf> which only takes and yields
	    values from <l> """
	fzl = frozenset(l)
	lut = dict()
	for i in l:
		lut[i] = fzl.intersection(succsOf(i))
	return lambda x: lut[x]

def dual_cover(l, succsOf):
	""" <succsOf> assigns to each element of <l> a list of successors.
	    This function returns the dual, "predsOf" if you will. """ 
	lut = dict()
	for i in l:
		lut[i] = list()
	for i in l:
		for j in succsOf(i):
			lut[j].append(i)
	return lambda x: lut[x]
		
def sort_by_successors(l, succsOf):
	""" Sorts a list, such that if l[b] in succsOf(l[a]) then a < b """
	rlut = dict()
	nret = 0
	todo = list()
	for i in l:
		rlut[i] = set()
	for i in l:
		for j in succsOf(i):
			rlut[j].add(i)
	for i in l:
		if len(rlut[i]) == 0:
			todo.append(i)
	while len(todo) > 0:
		i = todo.pop()
		nret += 1
		yield i
		for j in succsOf(i):
			rlut[j].remove(i)
			if len(rlut[j]) == 0:
				todo.append(j)
	if nret != len(l):
		raise ValueError, "Cycle detected"

class InstanceInfo(object):
	def __init__(self):
		self.deps = dict()

class ModuleDefinition(object):
	def __init__(self):
		self.deps = dict()
		self.vsettings = dict()
		self.implementedBy = None
		self.run = False

class Manager(object):
	def __init__(self, logger=None):
		if logger is None:
			logger = logging.getLogger(object.__repr__(self))
		self.l = logger
		self.modules = dict()
		self.valueTypes = {'str': str,
				   'float': float,
				   'int': int}
		self.insts = dict()
	
	def add_module_definition(self, name, definition):
		if name in self.modules:
			raise ValueError, "Duplicate module name"
		self.modules[name] = definition
	
	def create_instance(self, name, moduleName, settings):
		""" Creates an instance of <moduleName> at <name> with
		    <settings>. """
		if name in self.insts:
			raise ValueError, \
				"There's already an instance named %s" % \
						name
		if not moduleName in self.modules:
			raise ValueError, \
				"There's no module %s" % moduleName
		md = self.modules[moduleName]
		ii = self.insts[name] = InstanceInfo()
		for k, v in md.deps.iteritems():
			if not k in settings:
				raise ValueError, "Missing setting %s" % k
			if not settings[k] in self.insts:
				raise ValueError, "No such instance %s" \
						% settings[k]
			ii.deps[k] = settings[k]
			settings[k] = self.insts[settings[k]].object
		for k, v in md.vsettings.iteritems():
			if not k in settings:
				self.l.warn('%s:%s not set' % 
						(name, k))
		cl = get_by_path(md.implementedBy)
		il = logging.getLogger(name)
		ii.settings = settings
		ii.module = moduleName
		self.l.info('create_instance %-15s %s' % (
				name, md.implementedBy))
		ii.object = cl(settings, il)
		if md.run:
			ii.thread = threading.Thread(target=ii.object.run)
			ii.thread.start()

def depsOf_of_mirteFile_instance_definition(man, insts):
	""" Returns a function that returns the dependencies of
	    an instance definition by its name, where insts is a
	    dictionary of instance definitions from a mirteFile """
	return lambda x: filter(lambda y: y in \
				man.modules[insts[x]['module']].deps,
				insts[x].keys())

def depsOf_of_mirteFile_module_definition(defs):
	""" Returns a function that returns the dependencies of a module
	    definition by its name, where defs is a dictionary of module
	    definitions from a mirteFile """
	return lambda x: (filter(lambda z: z in defs,
				 map(lambda y: y[1]['type'],
			      	     defs[x]['settings'].items()
				     	if 'settings' in defs[x] else []))) + \
			 (defs[x]['inherits'] if 'inherits' in defs[x] else [])

def module_definition_from_mirteFile_dict(man, d):
	m = ModuleDefinition()
	if not 'inherits' in d: d['inherits'] = list()
	if not 'settings' in d: d['settings'] = dict()
	if 'implementedBy' in d:
		m.implementedBy = d['implementedBy']
	if 'run' in d and d['run']:
		m.run = True
	for p in d['inherits']:
		if not p in man.modules:
			raise ValueError, "No such module %s" % p
		m.deps.update(man.modules[p].deps)
		m.vsettings.update(man.modules[p].vsettings)
		m.run = m.run or man.modules[p].run
	for k, v in d['settings'].iteritems():
		if v['type'] in man.modules:
			m.deps[k] = v['type']
		elif v['type'] in man.valueTypes:
			m.vsettings[k] = v['type']
		else:
			raise ValueError, \
				"No such module or valuetype %s" % v
	return m

def load_mirteFile(path, m, logger=None):
	l = logging.getLogger('load_mirteFile') if logger is None else logger
	for path, d in walk_mirteFiles(path):
		l.info('loading %s' % path)
		_load_mirteFile(d, m)

def _load_mirteFile(d, m):
	defs = d['definitions'] if 'definitions' in d else {}
	insts = d['instances'] if 'instances' in d else {}
	it = sort_by_successors(defs.keys(), dual_cover(defs.keys(),
		restricted_cover(defs.keys(),
				 depsOf_of_mirteFile_module_definition(defs))))
	for k in it:
		m.add_module_definition(k,
			module_definition_from_mirteFile_dict(m, defs[k]))
	it = sort_by_successors(insts.keys(),
		dual_cover(insts.keys(), restricted_cover(insts.keys(),
			depsOf_of_mirteFile_instance_definition(m, insts))))
	for k in it:
		settings = dict(insts[k])
		del(settings['module'])
		m.create_instance(k, insts[k]['module'], settings)

def walk_mirteFiles(path):
	stack = [path]
	loadStack = []
	while stack:
		path = stack.pop()
		with open(path) as f:
			d = yaml.load(f)
		loadStack.append((path, d))
		if not 'includes' in d:
			continue
		for include in d['includes']:
			stack.append(os.path.join(os.path.dirname(path),
						  include))
	for path, d in reversed(loadStack):
		yield path, d

def main():
	logging.basicConfig(level=logging.DEBUG)
	l = logging.getLogger('mirte')
	m = Manager(l)
	load_mirteFile('default.mirte.yaml', m, logger=l)

if __name__ == '__main__':
	main()
