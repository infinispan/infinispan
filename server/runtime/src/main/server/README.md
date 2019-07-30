${infinispan.brand.name} Server ${infinispan.brand.version}
====================================

Welcome to ${infinispan.brand.name} Server.

Getting Started
---------------
${infinispan.brand.name} Server requires JDK 1.8 or later.

Starting a Server
-----------------
${infinispan.brand.name} Server runs a single instance. The default configuration defines a *transport*, meaning that
it will automatically discover and join other nodes to form a cluster.

```
<HOME>/bin/server.sh      (Unix / Linux)

<HOME>\bin\server.bat     (Windows)
```

Configurations
--------------
The default configuration file is `<HOME>/server/conf/infinispan.xml`. You can select an alternate file via
the `-c` argument:

```
<HOME>/bin/server.sh -c infinispan-local.xml     (Unix / Linux)

<HOME>\bin\server.bat -c infinispan-local.xml    (Windows)
```
The above commands will start the server using the `<HOME>/server/conf/infinispan-local.xml` which does not have
a transport and, therefore, will not cluster with other nodes on the same network.

Bind address and port
---------------------
Once started, the server will be listening on port 11222 on your first interface (usually the loopback interface).
You can make it listen on another interface by using the `-b` argument:

```
<HOME>/bin/server.sh -b 0.0.0.0     (Unix / Linux)

<HOME>\bin\server.bat -b 0.0.0.0    (Windows)
```
The above commands will make the server bind on all addresses.

You can also choose an alternate port by using the `-p` argument:

```
<HOME>/bin/server.sh -p 30000     (Unix / Linux)

<HOME>\bin\server.bat -p 30000    (Windows)
```

Clustering stacks
-----------------
The default clustering stack (`tcp`) will use TCP to communicate between nodes in the cluster. You can use alternate stacks via
the `-j` argument:

```
<HOME>/bin/server.sh -j udp     (Unix / Linux)

<HOME>\bin\server.bat -j udp    (Windows)
```
The above commands will use the `udp` stack which uses UDP to communicate between the nodes. 

Stopping the Server
-------------------
To stop a server, simply interrupt it (Ctrl-C from the terminal it was launched or kill the process via the TERM signal)