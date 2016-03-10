package org.infinispan.server.test.util;

import org.wildfly.extras.creaper.core.online.OnlineManagementClient;
import org.wildfly.extras.creaper.core.online.OnlineOptions;
import org.wildfly.extras.creaper.core.online.operations.Address;
import org.wildfly.extras.creaper.core.online.operations.Operations;
import org.wildfly.extras.creaper.core.online.operations.Values;

import java.io.IOException;

/**
 * The client connects to a running WildFly/EAP server and performs operations on DMR using
 * a helper project Creaper (https://github.com/wildfly-extras/creaper/). The operations include
 * adding and removing caches, endpoints, sockets-bindings etc.
 *
 * @author mgencur
 */
public class ManagementClient {

    public static final String NODE0_ADDRESS = System.getProperty("node0.ip", "127.0.0.1");
    public static final int NODE0_PORT = Integer.valueOf(System.getProperty("node0.mgmt.port", "9990"));
    public static final String LOGIN = System.getProperty("login", "admin");
    public static final String PASSWORD = System.getProperty("password", "admin9Pass!");
    private static final int DEFAULT_JMX_PORT = 4447;

    private static ManagementClient client;
    private Operations ops;

    private ManagementClient(String mgmtAddress, int mgmtPort) {
        OnlineManagementClient onlineClient = null;
        try {
           onlineClient = org.wildfly.extras.creaper.core.ManagementClient.online(OnlineOptions.domain()
                             .forProfile("clustered")
                             .build()
                             .hostAndPort(mgmtAddress, mgmtPort)
                             .auth(LOGIN, PASSWORD)
                             .build()
                            );
       } catch (IOException ex) {
           throw new IllegalStateException("Error during connecting to server CLI.", ex);
       }
       ops = new Operations(onlineClient);
    }

    public static ManagementClient getInstance(String mgmtAddress, int mgmtPort) {
        if (client == null)
            client = new ManagementClient(mgmtAddress, mgmtPort);
        return client;
    }

    public static ManagementClient getInstance() {
        if (client == null)
            client = new ManagementClient(NODE0_ADDRESS, NODE0_PORT);
        return client;
    }

    public void addDistributedCache(String name, String cacheContainer, String baseConfiguration) throws Exception {
        addCacheConfiguration("distCacheConfiguration", cacheContainer, CacheTemplate.DIST);
        addCache(name, cacheContainer, baseConfiguration, "distributed-cache");
    }

    public void removeDistributedCache(String name, String cacheContainer) throws Exception {
        removeCache(name, cacheContainer, "distributed-cache");
    }

    public void addReplicatedCache(String name, String cacheContainer, String baseConfiguration) throws Exception {
        addCache(name, cacheContainer, baseConfiguration, "replicated-cache");
    }

    public void removeReplicatedCache(String name, String cacheContainer) throws Exception {
        removeCache(name, cacheContainer, "replicated-cache");
    }

    public void addLocalCache(String name, String cacheContainer, String baseConfiguration) throws Exception {
        addCacheConfiguration(baseConfiguration, cacheContainer, CacheTemplate.LOCAL);
        addCache(name, cacheContainer, baseConfiguration, "local-cache");
    }

    public void removeLocalCache(String name, String cacheContainer) throws Exception {
        removeCache(name, cacheContainer, "local-cache");
    }

    public void addCache(String name, String cacheContainer, String baseConfiguration, String cacheType) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and(cacheType, name),
                Values.empty()
                        .andOptional("configuration", baseConfiguration)
                        .and("start", "EAGER")
                        .and("mode", "SYNC"));
    }

    public void removeCache(String name, String cacheContainer, String cacheType) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan")
                .and("cache-container", cacheContainer)
                .and(cacheType, name));
    }


    public void addCacheConfiguration(String name, String cacheContainer, CacheTemplate template) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and("configurations", "CONFIGURATIONS")
                        .and(template.getType(), name),
                Values.empty()
                        .and("mode", "SYNC")
                        .andOptional("start", "EAGER"));
    }

    public void removeLocalCacheConfiguration(String name, String cacheContainer) throws Exception {
        removeCacheConfiguration(name, cacheContainer, CacheTemplate.LOCAL);
    }

    public void removeDistributedCacheConfiguration(String name, String cacheContainer) throws Exception {
        removeCacheConfiguration(name, cacheContainer, CacheTemplate.DIST);
    }

    public void removeReplicatedCacheConfiguration(String name, String cacheContainer) throws Exception {
        removeCacheConfiguration(name, cacheContainer, CacheTemplate.REPL);
    }
    public void removeCacheConfiguration(String name, String cacheContainer, CacheTemplate template) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan")
                .and("cache-container", cacheContainer)
                .and("configurations", "CONFIGURATIONS")
                .and(template.getType(), name));
    }

    public void addConfigurations(String cacheContainer) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and("configurations", "CONFIGURATIONS"),
                Values.empty());
    }

    public void removeConfigurations(String cacheContainer) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan")
                .and("cache-container", cacheContainer)
                .and("configurations", "CONFIGURATIONS"));
    }

    public void addSocketBinding(String name, String socketBindingGroup, int port) throws Exception {
        ops.add(Address.of("socket-binding-group", socketBindingGroup)
                        .and("socket-binding", name),
                Values.empty()
                        .and("port", port));
    }

    public void removeSocketBinding(String name, String socketBindingGroup) throws Exception {
        ops.removeIfExists(Address.of("socket-binding-group", socketBindingGroup)
                .and("socket-binding", name));
    }

    public void addRemotingConnector(String socketBinding) throws Exception {
        ops.add(Address.subsystem("remoting")
                        .and("connector", "remoting-connector"),
                Values.empty()
                        .and("socket-binding", socketBinding)
                        .and("security-realm", "ApplicationRealm"));
        ops.add(Address.subsystem("jmx")
                        .and("remoting-connector", "jmx"), //this has to be "jmx" name
                Values.empty()
                        .and("use-management-endpoint", "false"));
    }

    public void removeRemotingConnector(String socketBinding) throws Exception {
        ops.removeIfExists(Address.subsystem("jmx")
                .and("remoting-connector", "jmx"));
        ops.removeIfExists(Address.subsystem("remoting")
                .and("connector", "remoting-connector"));
    }

    public void enableJmx() throws Exception {
        addSocketBinding("remoting", "clustered-sockets", DEFAULT_JMX_PORT);
        addRemotingConnector("remoting");
    }

    public void disableJmx() throws Exception {
        removeRemotingConnector("remoting");
        removeSocketBinding("remoting", "clustered-sockets");
    }

    public void addMemcachedEndpoint(String name, String cacheContainer, String cache, String socketBinding) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("memcached-connector", name),
                Values.empty()
                        .and("cache-container", cacheContainer)
                        .and("cache", cache)
                        .and("socket-binding", socketBinding));
    }

    public void removeMemcachedEndpoint(String name) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan-endpoint")
                .and("memcached-connector", name));
    }

    public void addRestEndpoint(String name, String cacheContainer, String cache, String socketBinding) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("rest-connector", name),
                Values.empty()
                        .and("cache-container", cacheContainer)
                        .and("cache", cache)
                        .and("socket-binding", socketBinding));
    }

    public void removeRestEndpoint(String name) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan-endpoint")
                .and("rest-connector", name));
    }

    public void addHotRodEndpoint(String name, String cacheContainer, String cache, String socketBinding) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("hotrod-connector", name),
                Values.empty()
                        .and("cache-container", cacheContainer)
                        .and("cache", cache)
                        .and("socket-binding", socketBinding));
    }

    public void removeHotRodEndpoint(String name) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan-endpoint")
                .and("hotrod-connector", name));
    }

    public void addCacheContainer(String name, String defaultCache) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", name),
                Values.empty()
                        .and("default-cache", defaultCache)
                        .and("statistics", "true"));
    }

    public void removeCacheContainer(String name) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan")
                .and("cache-container", name));
    }

    private enum CacheTemplate {
        DIST("distributed-cache-configuration"),
        REPL("replicated-cache-configuration"),
        LOCAL("local-cache-configuration");

        private String type;

        CacheTemplate(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

}