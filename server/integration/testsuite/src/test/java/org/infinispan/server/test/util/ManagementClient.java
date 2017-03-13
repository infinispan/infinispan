package org.infinispan.server.test.util;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.jboss.dmr.ModelNode;
import org.wildfly.extras.creaper.commands.security.realms.AddSecurityRealm;
import org.wildfly.extras.creaper.commands.security.realms.RemoveSecurityRealm;
import org.wildfly.extras.creaper.core.online.ModelNodeResult;
import org.wildfly.extras.creaper.core.online.OnlineManagementClient;
import org.wildfly.extras.creaper.core.online.OnlineOptions;
import org.wildfly.extras.creaper.core.online.operations.Address;
import org.wildfly.extras.creaper.core.online.operations.Operations;
import org.wildfly.extras.creaper.core.online.operations.Values;
import org.wildfly.extras.creaper.core.online.operations.admin.Administration;

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
    private static OnlineManagementClient internalOnlineClient;
    private Operations ops;

    private ManagementClient(String mgmtAddress, int mgmtPort, boolean standalone) {
        try {
            if (standalone) {
                internalOnlineClient = getStandaloneClient(mgmtAddress, mgmtPort);
            } else {
                internalOnlineClient = getDomainClient(mgmtAddress, mgmtPort);
            }
       } catch (IOException ex) {
           throw new IllegalStateException("Error during connecting to server CLI.", ex);
       }
       ops = new Operations(internalOnlineClient);
    }

    private static OnlineManagementClient getDomainClient(String mgmtAddress, int mgmtPort) throws IOException {
        internalOnlineClient = org.wildfly.extras.creaper.core.ManagementClient.online(OnlineOptions.domain()
                            .forProfile("clustered")
                            .build()
                        .hostAndPort(mgmtAddress, mgmtPort)
                        .auth(LOGIN, PASSWORD)
                        .build()
        );
        return internalOnlineClient;
    }

    private static OnlineManagementClient getStandaloneClient(String mgmtAddress, int mgmtPort) throws IOException {
        internalOnlineClient = org.wildfly.extras.creaper.core.ManagementClient.online(OnlineOptions.standalone()
                        .hostAndPort(mgmtAddress, mgmtPort)
                        .auth(LOGIN, PASSWORD)
                        .build()
        );
        return internalOnlineClient;
    }

    public static ManagementClient getStandaloneInstance() {
        if (client == null)
            client = new ManagementClient(NODE0_ADDRESS, NODE0_PORT, true);
        return client;
    }

    public static ManagementClient getInstance() {
        if (client == null)
            client = new ManagementClient(NODE0_ADDRESS, NODE0_PORT, false);
        return client;
    }

    public void addDistributedCache(String name, String cacheContainer, String baseConfiguration) throws Exception {
        addCache(name, cacheContainer, baseConfiguration, CacheType.DIST);
    }

    public void removeDistributedCache(String name, String cacheContainer) throws Exception {
        removeCache(name, cacheContainer, CacheType.DIST);
    }

    public void addReplicatedCache(String name, String cacheContainer, String baseConfiguration) throws Exception {
        addCache(name, cacheContainer, baseConfiguration, CacheType.REPL);
    }

    public void removeReplicatedCache(String name, String cacheContainer) throws Exception {
        removeCache(name, cacheContainer, CacheType.REPL);
    }

    public void addLocalCache(String name, String cacheContainer, String baseConfiguration) throws Exception {
        addCacheConfiguration(baseConfiguration, cacheContainer, CacheTemplate.LOCAL);
        addCache(name, cacheContainer, baseConfiguration, CacheType.LOCAL);
    }

    public void addCustomCacheStore(String cacheContainer, String cacheConfiguration, CacheTemplate template,  String cacheStoreName, String cacheStoreClass, Map<String, String> props) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and("configurations", "CONFIGURATIONS")
                        .and(template.getType(), cacheConfiguration)
                        .and("store", cacheStoreName),
                Values.empty()
                        .and("class", cacheStoreClass));

        for (Map.Entry<String, String> e: props.entrySet()) {
            ops.add(Address.subsystem("datagrid-infinispan")
                            .and("cache-container", cacheContainer)
                            .and("configurations", "CONFIGURATIONS")
                            .and(template.getType(), cacheConfiguration)
                            .and("store", cacheStoreName)
                            .and("property", e.getKey()),
                    Values.empty()
                            .and("value", e.getValue()));
        }
    }

    public void removeLocalCache(String name, String cacheContainer) throws Exception {
        removeCache(name, cacheContainer, CacheType.LOCAL);
    }

    public void addCache(String name, String cacheContainer, String baseConfiguration, CacheType cacheType) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and(cacheType.getType(), name),
                Values.empty()
                        .and("configuration", baseConfiguration)
                        .and("start", "EAGER"));
                     //   .and("mode", "SYNC"));
    }

    public void removeCache(String name, String cacheContainer, CacheType cacheType) throws Exception {
        ops.removeIfExists(Address.subsystem("datagrid-infinispan")
                .and("cache-container", cacheContainer)
                .and(cacheType.getType(), name));
    }


    public void addCacheConfiguration(String name, String cacheContainer, CacheTemplate template) throws Exception {
        addConfigurations(cacheContainer);
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and("configurations", "CONFIGURATIONS")
                        .and(template.getType(), name),
                Values.empty()
                        .and("statistics", "true")
                        .andOptional("start", "EAGER"));
    }

    public void addDistributedCacheConfiguration(String name, String cacheContainer) throws Exception {
        addCacheConfiguration(name, cacheContainer, CacheTemplate.DIST);
    }

    public void addReplicatedCacheConfiguration(String name, String cacheContainer) throws Exception {
        addCacheConfiguration(name, cacheContainer, CacheTemplate.REPL);
    }

    public void enableObjectEvictionForConfiguration(String cacheContainer, String cacheConfiguration, CacheTemplate template, int size) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", cacheContainer)
                        .and("configurations", "CONFIGURATIONS")
                        .and(template.getType(), cacheConfiguration)
                        .and("memory", "OBJECT"),
                Values.empty()
                        .and("size", size));
    }

    public void enableTransactionForDistConfiguration(String configurationName, String containerName, Map<String, String> txAttr) throws Exception {
        enableTransactionConfiguration(configurationName, containerName, txAttr, CacheTemplate.DIST);
    }

    public void enableTransactionForReplConfiguration(String configurationName, String containerName, Map<String, String> txAttr) throws Exception {
        enableTransactionConfiguration(configurationName, containerName, txAttr, CacheTemplate.REPL);
    }

    private void enableTransactionConfiguration(String configurationName, String containerName, Map<String, String> txAttr, CacheTemplate template) throws IOException {
        //Adding transaction conf to created cache configuration
        ops.add(Address.subsystem("datagrid-infinispan")
                .and("cache-container", containerName)
                .and("configurations", "CONFIGURATIONS")
                .and(template.getType(), configurationName)
                .and("transaction", "TRANSACTION"));

        //Adding attributes to transaction
        for (Map.Entry<String, String> attr : txAttr.entrySet()) {
            ops.writeAttribute(Address.subsystem("datagrid-infinispan")
                    .and("cache-container", containerName)
                    .and("configurations", "CONFIGURATIONS")
                    .and(template.getType(), configurationName)
                    .and("transaction", "TRANSACTION"), attr.getKey(), attr.getValue());
        }
    }

    public void enableCompatibilityForDistConfiguration(String configurationName, String containerName) throws Exception {
        enableCompatibilityForConfiguration(configurationName, containerName, CacheTemplate.DIST);
    }

    public void enableCompatibilityForReplConfiguration(String configurationName, String containerName) throws Exception {
        enableCompatibilityForConfiguration(configurationName, containerName, CacheTemplate.REPL);
    }

    public void enableCompatibilityForConfiguration(String configurationName, String containerName, CacheTemplate template) throws Exception {
        //Adding compatibility conf to created cache configuration
        ops.add(Address.subsystem("datagrid-infinispan")
                .and("cache-container", containerName)
                .and("configurations", "CONFIGURATIONS")
                .and(template.getType(), configurationName)
                .and("compatibility", "COMPATIBILITY"));

        //Enabling compatibility
        ops.writeAttribute(Address.subsystem("datagrid-infinispan")
                .and("cache-container", containerName)
                .and("configurations", "CONFIGURATIONS")
                .and(template.getType(), configurationName)
                .and("compatibility", "COMPATIBILITY"), "enabled", true);
    }

    public void enableIndexingForConfiguration(String configurationName, String containerName, CacheTemplate template, IndexingType indexingType, Map<String, String> properties) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan")
                        .and("cache-container", containerName)
                        .and("configurations", "CONFIGURATIONS")
                        .and(template.getType(), configurationName)
                        .and("indexing", "INDEXING"),
                Values.empty()
                        .and("indexing", indexingType.getType())
                        .andObject("indexing-properties", Values.fromMap(properties))
        );
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
                        .and("socket-binding", socketBinding)
                        .and("name", name));
    }

    public void addRestAuthentication(String endpointName, String securityRealm, String authMethod) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("rest-connector", endpointName)
                        .and("authentication", "AUTHENTICATION"),
                Values.empty()
                        .and("security-realm", securityRealm)
                        .and("auth-method", authMethod));
    }

    public void addRestEncryption(String endpointName, String securityRealm, boolean requireSslClientAuth) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("rest-connector", endpointName)
                        .and("encryption", "ENCRYPTION"),
                Values.empty()
                        .and("security-realm", securityRealm)
                        .and("require-ssl-client-auth", requireSslClientAuth));
    }

    public void addHotRodEncryption(String endpointName, String securityRealm, boolean requireSslClientAuth) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("hotrod-connector", endpointName)
                        .and("encryption", "ENCRYPTION"),
                Values.empty()
                        .and("security-realm", securityRealm)
                        .and("require-ssl-client-auth", requireSslClientAuth));
    }

    public void addRestEncryptionSNI(String endpointName, String sniName, String hostName, String securityRealm) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("rest-connector", endpointName)
                        .and("encryption", "ENCRYPTION")
                        .and("sni", sniName),
                Values.empty()
                        .and("host-name", hostName));
        if (securityRealm != null) {
            ops.writeAttribute(Address.subsystem("datagrid-infinispan-endpoint")
                            .and("rest-connector", endpointName)
                            .and("encryption", "ENCRYPTION")
                            .and("sni", sniName),
                    "security-realm", securityRealm);
        }
    }

    public void addHotRodEncryptionSNI(String endpointName, String sniName, String hostName, String securityRealm) throws Exception {
        ops.add(Address.subsystem("datagrid-infinispan-endpoint")
                        .and("hotrod-connector", endpointName)
                        .and("encryption", "ENCRYPTION")
                        .and("sni", sniName),
                Values.empty()
                        .and("host-name", hostName));
        if (securityRealm != null) {
            ops.writeAttribute(Address.subsystem("datagrid-infinispan-endpoint")
                            .and("hotrod-connector", endpointName)
                            .and("encryption", "ENCRYPTION")
                            .and("sni", sniName),
                    "security-realm", securityRealm);
        }
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
                        .and("socket-binding", socketBinding)
                        .and("name", name));
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

    public void reloadMaster() throws IOException, TimeoutException, InterruptedException {
        Administration admin = new Administration(org.wildfly.extras.creaper.core.ManagementClient.online(OnlineOptions.domain()
                .forHost("master")
                .build()
                .hostAndPort(NODE0_ADDRESS, NODE0_PORT)
                .auth(LOGIN, PASSWORD)
                .build()
        ));

        admin.reload();
    }

    public void reloadIfRequired() throws IOException, TimeoutException, InterruptedException {
        Administration admin = new Administration(internalOnlineClient);
        admin.reloadIfRequired();
    }

    public void reload() throws IOException, TimeoutException, InterruptedException {
        Administration admin = new Administration(internalOnlineClient);
        admin.reload();
    }

    public void addSecurityRealm(String realmName) throws Exception {
        internalOnlineClient.apply(new AddSecurityRealm.Builder(realmName).build());
    }

    public void removeSecurityRealm(String realmName) throws Exception {
        internalOnlineClient.apply(new RemoveSecurityRealm(realmName));
    }

    public void addServerIdentity(String realmName, String path, String relativeTo, String keystorePassword) throws Exception {
        ops.add(Address.coreService("management")
                        .and("security-realm", realmName)
                        .and("server-identity", "ssl"),
                Values.empty()
                        .and("keystore-path", path)
                        .and("keystore-relative-to", relativeTo)
                        .and("keystore-password", keystorePassword));
    }

    public void addVault(Map<String, String> vaultOptions) throws Exception {
        ops.add(Address.coreService("vault"),
                Values.empty()
                        .and("vault-options", getAttributeString(vaultOptions)));
    }

    private String getAttributeString(Map<String, String> properties) {
        StringBuilder bld = new StringBuilder();
        bld.append("[");
        for(Map.Entry<String, String> e: properties.entrySet()) {
            bld.append("(");
            bld.append("\"" + e.getKey() + "\" => ");
            bld.append("\"" + e.getValue() + "\"");
            bld.append("),");
        }
        bld.deleteCharAt(bld.length() - 1);
        bld.append("]");
        return bld.toString();
    }

    public void removeVault() throws Exception {
        ops.removeIfExists(Address.coreService("vault"));
    }

    public enum CacheTemplate {
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

    public enum CacheType {
        DIST("distributed-cache"),
        REPL("replicated-cache"),
        LOCAL("local-cache");

        private String type;

        CacheType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    public enum IndexingType {
        ALL("ALL"),
        LOCAL("LOCAL"),
        NONE("NONE"),
        PRIMARY_OWNER("PRIMARY_OWNER");

        private String type;

        IndexingType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}
