Infinispan Server
=================

Infinispan Server is a standalone server which exposes any number of caches 
to clients over a variety of protocols, including HotRod, Memcached and REST. 
The server itself is built on top of the robust foundation provided by JBoss 
AS 7.2, therefore delegating services such as configuration, datasources, 
transactions, logging and security to the respective subsystems. 
Because Infinispan Server is closely tied to the latest releases of 
Infinispan and JGroups, the subsystems which control these components are 
slightly different, in that they introduce new features and change some 
existing ones (e.g. cross-site replication, etc). For this reason, the 
configuration of these subsystems should use the Infinispan Server-specific 
schema.

Getting Started
---------------

To get started using the server, launch it using the bin/standalone.sh or 
bin/standalone.bat scripts depending on your platform. This will start a 
single-node server using the standalone/configuration/standalone.xml 
configuration file, with four endpoints, one for each of the supported 
protocols. These endpoints allow access to all of the caches configured in the 
Infinispan subsystem (apart from the Memcached endpoint which, because of the 
protocol's design, only allows access to a single cache). The server also 
comes with a script (clustered.sh/clustered.bat) which provides an easy way 
to start a clustered server by using the 
standalone/configuration/clustered.xml configuration file. If you start the 
server in clustered mode on multiple hosts, they should automatically 
discover each other using UDP multicast and form a cluster. If you want to 
start multiple nodes on a single host, start each one by specifying a port 
offset using the jboss.socket.binding.port-offset property together with a 
unique jboss.node.name as follows:

bin/clustered.sh -Djboss.socket.binding.port-offset=100 -Djboss.node.name=nodeA

If, for some reason, you cannot use UDP multicast, you can use TCP discovery. 
Read the JGroups subsystem configuration section for details on how to 
configure TCP discovery.

The server distribution also provides a set of example configuration files in 
the docs/examples/configs which illustrate a variety of possible configurations 
and use-cases. To use them, just copy them to the standalone/configuration 
directory and start the server using the following syntax:

bin/standalone.sh -c configuration_file_name.xml


For more information on Infinispan Server, consult the documentation at:

https://docs.jboss.org/author/display/ISPN/Infinispan+Server

