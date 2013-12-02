#!/usr/bin/python

import sys
import xml.etree.ElementTree as ET

class Server:
	def __init__(self, path):
		doc = ET.parse(path)
		self.path = path
		self.extensions = []
		extensions = doc.findall('{0}extensions/{0}extension'.format("{urn:jboss:domain:1.0}"))
		for extension in extensions:
			self.extensions.append(extension.get('module'))

	def show(self):
		for e in self.extensions:
			print "  + %s" % e

	def traverse(self, allModules):
		modules = set()
		for d in self.extensions:
			try:
				module = allModules[d]
				module.traverse(modules, allModules)
			except KeyError:
				print "WARNING: missing module %s" % d
		return modules

class Module:
	def __init__(self, path):
		doc = ET.parse(path)
		root = doc.getroot()
		self.path = path	
		self.name = root.get('name')
		self.dependencies = []
		dependencies = doc.findall('{0}dependencies/{0}module'.format("{urn:jboss:module:1.0}"))
		for dependency in dependencies:
			self.dependencies.append(dependency.get('name'))

	def __hash__(self):
		return self.name.__hash__()

	def __eq__(self,other):
		return self.name.__eq__(other.name)

	def __cmp__(self,other):
		return self.name.__cmp__(other.name)

	def show(self):
		print self.name
		for d in self.dependencies:
			print "  L %s" % d

	def traverse(self, modules, allModules):
		if(self.name not in modules):
			modules.add(self.name)
			for d in self.dependencies:
				try:
					module = allModules[d]
					module.traverse(modules, allModules)
				except KeyError:
					print "WARNING: missing module %s" % d
		


# The main code
if len(sys.argv) < 3:
    sys.exit('Usage: %s standalone.xml module.xml [module.xml ...]' % sys.argv[0])

# Parse the server
server = Server(sys.argv[1])

# Parse all of the modules
availableModules = {}
for i in range(2, len(sys.argv)):
	module = Module(sys.argv[i])
	availableModules[module.name] = module

# Traverse all modules, building a set of required ones starting from the server extensions
requiredModules = server.traverse(availableModules)
print "Required modules"
for module in sorted(requiredModules):
	print "  %s" % module

print "Removable modules"
for module in sorted(availableModules):
	if (module not in requiredModules):
		print "  %s" % module
