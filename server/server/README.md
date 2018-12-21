Infinispan ServerNG
====================

Infinispan ServerNG is a reboot of Infinispan's server which addresses the following:

* small codebase with as little duplication of already existing functionality (e.g. configuration)
* embeddable: the server should allow for easy testability in both single and clustered configurations
* RESTful admin capabilities
* Logging using [JBoss Logging logmanager](https://github.com/jboss-logging/jboss-logmanager)
* Security using [Elytron](https://github.com/wildfly-security/wildfly-elytron)

# Layout

The server is laid out as follows:

* `/bin` scripts
   * `server.sh` server startup shell script for Unix/Linux
   * `server.ps1` server startup script for Windows Powershell 
* `/lib` server jars
   * `infinispan-server.jar` uber-jar of all dependencies required to run the server.
* `/server` default server instance folder
* `/server/log` log files
* `/server/configuration` configuration files
   * `infinispan.xml`
   * keystores
   * `logging.properties` for configuring logging
   * User/groups property files (e.g. `mgmt-users.properties`, `mgmt-groups.properties`) 
* `/server/data` data files organized by container name
   * `default`
      * `caches.xml` runtime cache configuration
      * `___global.state` global state
      * `mycache` cachestore data
* `/server/lib` extension jars (custom filter, listeners, etc)

# Paths

The following is a list of _paths_ which matter to the server:

* `infinispan.server.home` defaults to the directory which contains the server distribution.
* `infinispan.server.root` defaults to the `server` directory under the `infinispan.server.home`. Multiple roots can 
coexist under the same installation (or even ) outside the server home. The server root is locked when in use to avoid
concurrent uses by multiple server instances.
* `infinispan.server.configuration` defaults to `infinispan.xml` and is located in the `configuration` folder under the `infinispan.server.root`

# Command-line

The server supports the following command-line arguments:

* `-b`, `--bind-address=<address>` Sets the default bind address for the public interface.
* `-c`, `--server-config=<config>` Sets the `infinispan.server.configuration` path.
* `-o`, `--port-offset=<offset>` Adds the specified offset to all ports. 
* `-s`, `--server-root=<path>` Sets the `infinispan.server.root` path.
* `-v`, `--version` Displays the server version and exits.

# Configuration

The server configuration extends the standard Infinispan configuration adding server-specific elements:

* `security` configures the available security realms which can be used by the endpoints.
* `cache-container` multiple containers may be configured, distinguished by name.
* `endpoints` lists the enabled endpoint connectors (hotrod, rest, ...).
* `socket-bindings` lists the socket bindings.

An example configuration file looks as follows:

```
<infinispan
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="urn:infinispan:config:10.0 http://www.infinispan.org/schemas/infinispan-config-10.0.xsd
                            urn:infinispan:server:10.0 http://www.infinispan.org/schemas/infinispan-server-10.0.xsd"
        xmlns="urn:infinispan:config:10.0"
        xmlns:server="urn:infinispan:server:10.0">

   <cache-container default-cache="default" statistics="false">
      <transport cluster="${infinispan.cluster.name}" stack="udp"/>
      <global-state/>
      <distributed-cache name="default"/>
   </cache-container>

   <server xmlns="urn:infinispan:server:10.0">
      <interfaces>
         <interface name="public">
            <loopback/>
         </interface>
         <interface name="admin">
            <loopback/>
         </interface>
      </interfaces>

      <socket-bindings default-interface="public" port-offset="${infinispan.socket.binding.port-offset:0}">
         <socket-binding name="hotrod" port="11222"/>
         <socket-binding name="rest" port="8080"/>
         <socket-binding name="memcached" port="11221"/>
         <socket-binding name="admin" port="9990" interface="admin"/>
      </socket-bindings>

      <security>
         <security-realms>
            <security-realm name="ManagementRealm">
               <properties-realm groups-attribute="Roles">
                  <user-properties path="mgmt-users.properties" relative-to="infinispan.server.config.dir" plain-text="true"/>
                  <group-properties path="mgmt-groups.properties" relative-to="infinispan.server.config.dir" />
               </properties-realm>
            </security-realm>
            <security-realm name="PublicRealm">
               <properties-realm groups-attribute="Roles">
                  <user-properties path="public-users.properties" relative-to="infinispan.server.config.dir" plain-text="true"/>
                  <group-properties path="public-groups.properties" relative-to="infinispan.server.config.dir" />
               </properties-realm>
               <server-identities>
                  <ssl>
                     <keystore path="application.keystore" relative-to="infinispan.server.config.dir"
                               keystore-password="password" alias="server" key-password="password"
                               generate-self-signed-certificate-host="localhost"/>
                  </ssl>
               </server-identities>
            </security-realm>
         </security-realms>
      </security>

      <endpoints>
         <hotrod-connector socket-binding="hotrod"/>
         <memcached-connector socket-binding="memcached"/>
         <rest-connector socket-binding="rest"/>
      </endpoints>
   </server>
</infinispan>

```

# Logging

Logging is handled by JBoss Logging's LogManager. This is configured through a `logging.properties` file in the 
`server/configuration` directory. The following is an example:

```
loggers=org.jboss.logmanager

# Root logger
logger.level=INFO
logger.handlers=CONSOLE

logger.org.jboss.logmanager.useParentHandlers=true
logger.org.jboss.logmanager.level=INFO

handler.CONSOLE=org.jboss.logmanager.handlers.ConsoleHandler
handler.CONSOLE.formatter=PATTERN
handler.CONSOLE.properties=autoFlush,target
handler.CONSOLE.autoFlush=true
handler.CONSOLE.target=SYSTEM_OUT

formatter.PATTERN=org.jboss.logmanager.formatters.PatternFormatter
formatter.PATTERN.properties=pattern
formatter.PATTERN.pattern=%d{HH:mm:ss,SSS} %-5p [%c] (%t) %s%E%n
```
# Internals

The following is a dump of various internal facts about the server, in no particular order:

* All containers handled by the same server share the same thread pools and transport.
* When a server starts it locks the `infinispan.server.root` so that it cannot be used by another server concurrently.
   
 