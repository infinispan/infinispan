#!/usr/bin/python

import sys
import argparse
import xml.etree.ElementTree as ET

JBOSS_DOMAIN_NS = '{urn:jboss:domain:1.3}'
JBOSS_MODULE_NS = '{urn:jboss:module:1.1}'

class Server:
    def __init__(self, path):
        doc = ET.parse(path)
        self.path = path
        self.extensions = ['org.jboss.as.standalone', 'org.jboss.as.process-controller', 'org.jboss.as.host-controller']
        extensions = doc.findall('{0}extensions/{0}extension'.format(JBOSS_DOMAIN_NS))
        for extension in extensions:
            self.extensions.append(extension.get('module'))

    def show(self):
        for e in self.extensions:
            print >> sys.stderr, "  + %s" % e

    def traverse(self, allModules):
        modules = set()
        for d in self.extensions:
            try:
            	print >> sys.stderr, "Extension %s" % d
                module = allModules[d]
                module.traverse(modules, allModules)
            except KeyError:
                print >> sys.stderr, "WARNING: missing module %s" % d
        return modules

class Module:
    def __init__(self, path):
        doc = ET.parse(path)
        root = doc.getroot()
        self.path = path    
        self.name = root.get('name')
        self.dependencies = []
        self.resources = []
        if(root.tag==(JBOSS_MODULE_NS+'module-alias')):
            self.dependencies.append(root.get('target-name'))
        
        dependencies = doc.findall('{0}dependencies/{0}module'.format(JBOSS_MODULE_NS))
        for dependency in dependencies:
        	if(dependency.get('optional', 'false')=='false'):
	            self.dependencies.append(dependency.get('name'))
        resources = doc.findall('{0}resources/{0}resource-root'.format(JBOSS_MODULE_NS))
        for resource in resources:
            self.resources.append(resource.get('path'))

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self,other):
        return self.name.__eq__(other.name)

    def __cmp__(self,other):
        return self.name.__cmp__(other.name)

    def show(self):
        print >> sys.stderr, "%s [ " % self.name,
        for d in self.dependencies:
            print >> sys.stderr, "%s " % d,
        print >> sys.stderr, "]"

    def traverse(self, modules, allModules, indent=2):
        if(self.name not in modules):
            modules.add(self.name)
            print >> sys.stderr, self.name.rjust(len(self.name)+indent)
            for d in self.dependencies:
                try:
                    module = allModules[d]
                    module.traverse(modules, allModules, indent+2)
                except KeyError:
                    continue
        


# The main code
def main():
    parser = argparse.ArgumentParser(description='Dependency tool for JBoss AS/EAP modules')
    parser.add_argument('serverConfig', metavar='standalone.xml', type=str, help='The xml configuration file which lists the required extensions [e.g. standalone.xml / domain.xml]')
    parser.add_argument('modules', metavar='module.xml', type=str, nargs='+', help='A list of module.xml definition files')
    parser.add_argument('--includes', dest='mode', action='store_const', const='includes', default='excludes', help='Prints out the modules to be included (default is to print out the modules to be excluded)')
    parser.add_argument('--html', dest='mode', action='store_const', const='html', default='excludes', help='Produces an HTML version of the dependency relationships')

    args = parser.parse_args()
    
    # Parse the server
    server = Server(args.serverConfig)

    # Parse all of the modules
    availableModules = {}
    for i in args.modules:
        module = Module(i)
        availableModules[module.name] = module

    # Traverse all modules, building a set of required ones starting from the server extensions
    requiredModules = server.traverse(availableModules)

    if args.mode == 'includes':    
        for module in sorted(requiredModules):
            availableModules[module].show()
    elif args.mode == 'excludes':
        for module in sorted(availableModules):
            if (module not in requiredModules):
                print module
    elif args.mode == 'html':
        print '<!DOCTYPE HTML>\n<html>\n<head>\n<title>EDG dependencies</title>\n<style type="text/css">'
        print 'ul {margin: 0}'
        print 'td {border: 1px solid black}'
        print '</style>\n</head>\n<body>'
        print '<table style="border-collapse: collapse">'
        print '<tr><th>Module</th><th>Depends on</th><th>Is a dependency of</th><th>Resources</th></tr>'
        for module in sorted(availableModules):
            color = '#aaffaa' if (module in requiredModules) else '#ffaaaa'
            print '<tr style="background: %s"><td><a name="%s"><h4>%s</h4></a></td>' % (color, module, module)
            deps = availableModules[module].dependencies

			# Depends on
            print '<td><ul>'
            for d in deps:
                print '<li><a href=\'#%s\'>%s</a></li>' % (d, d)
            print '</ul></td>'
            reqs = set()
            for r in availableModules:
                if (module in availableModules[r].dependencies):
                    reqs.add(r)
			# Dependency of
            print '<td><ul>'
            for r in sorted(reqs):
                print '<li><a href=\'#%s\'>%s</a></li>' % (r, r)
            print '</ul></td>'
            # Resources
            rsrcs = availableModules[module].resources
            print '<td><ul>'
            for r in rsrcs:
            	print '<li>%s</li>' % (r)
            print '</ul></td>'
            print '</tr>'
        print '</table>\n'
        print '</body>\n</html>\n'


main()

