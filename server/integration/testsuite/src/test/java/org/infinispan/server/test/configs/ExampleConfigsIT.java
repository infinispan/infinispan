package org.infinispan.server.test.configs;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_B;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_C;
import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.SERVER2_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.invokeOperation;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.infinispan.server.test.util.ITestUtils.stopContainers;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;
import javax.net.ssl.SSLContext;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.jboss.arquillian.container.test.api.Config;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for example configurations.
 *
 * @author Jakub Markos (jmarkos@redhat.com)
 * @author Galder Zamarreño (galderz@redhat.com)
 * @author Michal Linhard (mlinhard@redhat.com)
 */
@RunWith(Arquillian.class)
public class ExampleConfigsIT {

    protected static final String KEYSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
          + "keystore_client.jks";
    protected static final String TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
          + "ca.jks";
    protected static final String KEYSTORE_PASSWORD = "secret";

    private static final Logger log = Logger.getLogger(ExampleConfigsIT.class);

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    static final String DEFAULT_CACHE_NAME = "default";
    static final String NAMED_CACHE_NAME = "namedCache";

    @ArquillianResource
    ContainerController controller;

    RemoteCacheManagerFactory rcmFactory;

    @Before
    public void setUp() {
        rcmFactory = new RemoteCacheManagerFactory();
    }

    @After
    public void tearDown() {
        if (rcmFactory != null) {
            rcmFactory.stopManagers();
        }
        rcmFactory = null;
    }

    @Test
    @WithRunningServer({@RunningServer(name = "example-hotrod-rolling-upgrade-2"),@RunningServer(name = "example-hotrod-rolling-upgrade-1")})
    public void testHotRodRollingUpgrades() throws Exception {
        // Target node
        MBeanServerConnectionProvider provider1;
        // Source node
        MBeanServerConnectionProvider provider2;

        RemoteInfinispanMBeans s2 = createRemotes("example-hotrod-rolling-upgrade-2", "local", DEFAULT_CACHE_NAME);
        final RemoteCache<Object, Object> c2 = createCache(s2);

        c2.put("key1", "value1");
        assertEquals("value1", c2.get("key1"));

        for (int i = 0; i < 50; i++) {
            c2.put("keyLoad" + i, "valueLoad" + i);
        }

        controller.start("example-hotrod-rolling-upgrade-1");

        RemoteInfinispanMBeans s1 = createRemotes("example-hotrod-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
        final RemoteCache<Object, Object> c1 = createCache(s1);

        assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", c1.get("key1"));

        provider1 = new MBeanServerConnectionProvider(s1.server.getHotrodEndpoint().getInetAddress().getHostName(),
                                                      SERVER1_MGMT_PORT);
        provider2 = new MBeanServerConnectionProvider(s2.server.getHotrodEndpoint().getInetAddress().getHostName(),
                                                      SERVER2_MGMT_PORT);

        final ObjectName rollMan = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache," + "name=\"default(local)\","
                + "manager=\"local\"," + "component=RollingUpgradeManager");

        invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"hotrod"},
                new String[]{"java.lang.String"});

        invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"hotrod"},
                new String[]{"java.lang.String"});

        // is source (RemoteCacheStore) really disconnected?
        c2.put("disconnected", "source");
        assertEquals("Can't obtain value from cache1 (source node).", "source", c2.get("disconnected"));
        assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                c1.get("disconnected"));

        // all entries migrated?
        assertEquals("Entry was not successfully migrated.", "value1", c1.get("key1"));
        for (int i = 0; i < 50; i++) {
            assertEquals("Entry was not successfully migrated.", "valueLoad" + i, c1.get("keyLoad" + i));
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = "rest-rolling-upgrade-2"),@RunningServer(name = "rest-rolling-upgrade-1")})
    public void testRestRollingUpgrades() throws Exception {
        final int PORT_OFFSET = 100;
        // target node
        MBeanServerConnectionProvider provider1;
        // Source node
        MBeanServerConnectionProvider provider2;

        RESTHelper rest = new RESTHelper();

        controller.start("rest-rolling-upgrade-2");
        // Source server is in compat mode
        try {
            RemoteInfinispanMBeans s2 = createRemotes("rest-rolling-upgrade-2", "local", DEFAULT_CACHE_NAME);
            rest.addServer(s2.server.getRESTEndpoint().getInetAddress().getHostName(), s2.server.getRESTEndpoint().getContextPath());

            rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET), "data", "text/plain");
            rest.get(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "key1", PORT_OFFSET), "data", 200, false, "Accept", "text/plain");

            for (int i = 0; i < 50; i++) {
                rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "keyLoad" + i, PORT_OFFSET), "valueLoad" + i, "text/plain");
            }

            controller.start("rest-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("rest-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            rest.addServer(s1.server.getRESTEndpoint().getInetAddress().getHostName(), s1.server.getRESTEndpoint().getContextPath());

            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "key1", 0), "data", 200, false, "Accept", "text/plain");

            provider1 = new MBeanServerConnectionProvider(s1.server.getRESTEndpoint().getInetAddress().getHostName(),
                                                          SERVER1_MGMT_PORT);

            final ObjectName rollMan = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache," + "name=\"default(local)\","
                                                            + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"rest"},
                            new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"rest"},
                            new String[]{"java.lang.String"});

            rest.post(rest.fullPathKey(0, DEFAULT_CACHE_NAME, "disconnected", PORT_OFFSET), "source", "text/plain");

            //Source node entries should NOT be accessible from target node
            rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "disconnected", 0), HttpStatus.SC_NOT_FOUND);

            //All remaining entries migrated?
            for (int i = 0; i < 50; i++) {
                rest.get(rest.fullPathKey(1, DEFAULT_CACHE_NAME, "keyLoad" + i, 0), "valueLoad" + i);
            }
        } finally {
            if (controller.isStarted("rest-rolling-upgrade-1")) {
                controller.stop("rest-rolling-upgrade-1");
            }
            if (controller.isStarted("rest-rolling-upgrade-2")) {
                controller.stop("rest-rolling-upgrade-2");
            }
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-compatibility-mode")})
    public void testCompatibilityModeConfig() throws Exception {
        MemcachedClient memcachedClient = null;
        CloseableHttpClient restClient = null;
        try {
            RemoteInfinispanMBeans s1 = createRemotes("standalone-compatibility-mode", "local", DEFAULT_CACHE_NAME);
            RemoteCache<Object, Object> s1Cache = createCache(s1);
            restClient = HttpClients.createDefault();
            String restUrl = "http://" + s1.server.getHotrodEndpoint().getInetAddress().getHostName() + ":8080"
                    + s1.server.getRESTEndpoint().getContextPath() + "/" + DEFAULT_CACHE_NAME;
            memcachedClient = new MemcachedClient(s1.server.getMemcachedEndpoint().getInetAddress().getHostName(), s1.server
                    .getMemcachedEndpoint().getPort());
            String key = "1";

            // 1. Put with Hot Rod
            assertEquals(null, s1Cache.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1".getBytes()));
            assertArrayEquals("v1".getBytes(), (byte[]) s1Cache.get(key));

            // 2. Get with REST
            HttpGet get = new HttpGet(restUrl + "/" + key);
            get.addHeader("Accept", ContentType.APPLICATION_OCTET_STREAM.toString());
            HttpResponse getResponse = restClient.execute(get);
            assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
            assertArrayEquals("v1".getBytes(), EntityUtils.toByteArray(getResponse.getEntity()));

            // 3. Get with Memcached
            assertArrayEquals("v1".getBytes(), readWithMemcachedAndDeserialize(key, memcachedClient));

            key = "2";

            // 1. Put with REST
            HttpPut put = new HttpPut(restUrl + "/" + key);
            put.setEntity(new ByteArrayEntity("<hey>ho</hey>".getBytes(), ContentType.APPLICATION_OCTET_STREAM));
            HttpResponse putResponse = restClient.execute(put);
            assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

            // 2. Get with Hot Rod
            assertArrayEquals("<hey>ho</hey>".getBytes(), (byte[]) s1Cache.get(key));

            // 3. Get with Memcached
            assertArrayEquals("<hey>ho</hey>".getBytes(), readWithMemcachedAndDeserialize(key, memcachedClient));
        } finally {
            if (restClient != null) {
                restClient.close();
            }
            if (memcachedClient != null) {
                memcachedClient.close();
            }
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-fcs-local")})
    public void testFileCacheStoreConfig() throws Exception {
        doPutGetCheckPath(createRemotes("standalone-fcs-local", "local", DEFAULT_CACHE_NAME), "dc", -1.0);
        doPutGetCheckPath(createRemotes("standalone-fcs-local", "local", NAMED_CACHE_NAME), "nc", 2.1);
    }

    @Test
    @WithRunningServer({@RunningServer(name = "clustered-jdbc")})
    public void testJDBCCacheStoreConfig() throws Exception {
        RemoteInfinispanMBeans sDefault = createRemotes("clustered-jdbc", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans sNamed = createRemotes("clustered-jdbc", "clustered", NAMED_CACHE_NAME);
        RemoteCache<Object, Object> sDefaultCache = createCache(sDefault);
        RemoteCache<Object, Object> sNamedCache = createCache(sNamed);
        sNamedCache.put("key", "value");
        sNamedCache.put("key2", "value2");
        assertEquals("value", sNamedCache.get("key"));
        assertEquals("value2", sNamedCache.get("key2"));

        // 1001, so we are 100% sure that at least 1 entry is evicted and thus stored (passivation = true)
        for (int i = 0; i < 1001; i++) {
            sDefaultCache.put("k" + i, "v" + i);
        }
        for (int i = 0; i < 1001; i++) {
            assertEquals("v" + i, sDefaultCache.get("k" + i));
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-rocksdb-cs-local")})
    public void testRocksDBCacheStoreConfig() throws Exception {
        doPutGetCheckPath(createRemotes("standalone-rocksdb-cs-local", "local", DEFAULT_CACHE_NAME), "rocksdb-dcdefault", -1.0);
        doPutGetCheckPath(createRemotes("standalone-rocksdb-cs-local", "local", NAMED_CACHE_NAME), "rocksdb-ncnamedCache", 2.1);
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-hotrod-multiple")})
    public void testHotrodMultipleConfig() throws Exception {
        RemoteInfinispanMBeans s = createRemotes("standalone-hotrod-multiple", "local", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> c1 = createCache(s, ITestUtils.createConfigBuilder(s.server.getHotrodEndpoint("external")
                .getInetAddress().getHostName(), s.server.getHotrodEndpoint("external").getPort()));
        RemoteCache<Object, Object> c2 = createCache(s, ITestUtils.createConfigBuilder(s.server.getHotrodEndpoint("internal")
                .getInetAddress().getHostName(), s.server.getHotrodEndpoint("internal").getPort()));
        assertEquals(0, s.cache.getNumberOfEntries());
        for (int i = 0; i < 10; i++) {
            c1.put("k" + i, "v" + i);
        }
        assertTrue(s.cache.getNumberOfEntries() <= 10);
        for (int i = 0; i < 10; i++) {
            assertEquals("v" + i, c2.get("k" + i));
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-rcs-local-2"),@RunningServer(name = "standalone-rcs-local-1")})
    public void testRemoteCacheStoreConfig() throws Exception {
        RemoteInfinispanMBeans sRemoteStoreDefault = createRemotes("standalone-rcs-local-2", "local", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans sRemoteStoreNamed = createRemotes("standalone-rcs-local-2", "local", NAMED_CACHE_NAME);
        RemoteInfinispanMBeans s1Default = createRemotes("standalone-rcs-local-1", "local", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s1Named = createRemotes("standalone-rcs-local-1", "local", NAMED_CACHE_NAME);

        doPutGetRemove(s1Default, sRemoteStoreDefault);
        doPutGetWithExpiration(s1Named, sRemoteStoreNamed);
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-hotrod-ssl")})
    public void testSSLHotRodConfig() throws Exception {
        RemoteInfinispanMBeans s = createRemotes("standalone-hotrod-ssl", "local", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> c = createCache(s, securityConfig("keystore_client.jks", "ca.jks", s.server));
        doPutGet(s, c);
        try {
            doPutGet(s, createCache(s, securityConfig("keystore_server_no_ca.jks", "ca.jks", s.server)));
            Assert.fail("Should have failed to write");
        } catch (TransportException e) {
            // ok
        }
    }

    @Test
    @WithRunningServer({@RunningServer(name = "standalone-rest-ssl")})
    public void testRestSslConfig() throws Exception {
        final RemoteInfinispanMBeans s = createRemotes("standalone-hotrod-ssl", "local", DEFAULT_CACHE_NAME);
        SSLContext sslContext = SslContextFactory.getContext(KEYSTORE_PATH, KEYSTORE_PASSWORD.toCharArray(), TRUSTSTORE_PATH, KEYSTORE_PASSWORD.toCharArray());

        RESTHelper rest = new RESTHelper();
        rest.withSslContext(sslContext).withPort(8443).withProtocol("https").withServer(s.server);

        cleanRESTServer(rest);
        HttpResponse response = rest.put(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        assertEquals(200, response.getStatusLine().getStatusCode());
        rest.get(rest.fullPathKey(0, KEY_A), "data");
        cleanRESTServer(rest);
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(0, KEY_A), "data");
        cleanRESTServer(rest);
    }

    @Test
    @WithRunningServer({@RunningServer(name = "clustered-storage-only-1"),@RunningServer(name = "clustered-storage-only-2")})
    public void testStorageOnlyConfig() throws Exception {
        RemoteInfinispanMBeans s1 = createRemotes("clustered-storage-only-1", "clustered", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> s1Cache = createCache(s1);
        assertEquals(0, s1.cache.getNumberOfEntries());
        assertEquals(2, s1.manager.getClusterSize());
        s1Cache.put("k", "v");
        s1Cache.put("k2", "v2");
        assertEquals(s1Cache.get("k"), "v");
        assertEquals(s1Cache.get("k2"), "v2");
        assertEquals(2, s1.cache.getNumberOfEntries());
        s1Cache.put("k3", "v3");
        assertEquals(3, s1.cache.getNumberOfEntries());
        assertEquals("v", s1Cache.get("k"));
        assertEquals("v2", s1Cache.get("k2"));
        assertEquals("v3", s1Cache.get("k3"));
        try {
            ITestUtils.createCacheManager(serverManager.getServer("clustered-storage-only-2"));
            Assert.fail();
        } catch (Exception e) {
            // OK - we are not able to access HotRod endpoint of storage-only node
        }
    }

    @Test
    public void testTopologyConfigMachineAttribute() throws Exception {
        try {
            startContainerWithTopology("clustered-topology-1", "node0", 0, "s1", "r1", "m1");
            startContainerWithTopology("clustered-topology-2", "node1", 100, "s1", "r1", "m1");
            startContainerWithTopology("clustered-topology-3", "node2", 200, "s1", "r1", "m2");

            verifyTopologyHinting("clustered-topology-1", "clustered-topology-2", "clustered-topology-3", "clustered", DEFAULT_CACHE_NAME);
        } finally {
            stopContainers(controller, "clustered-topology-1", "clustered-topology-2", "clustered-topology-3");
        }
    }

    @Test
    public void testTopologyConfigRackAttribute() throws Exception {
        try {
            startContainerWithTopology("clustered-topology-1", "node0", 0, "s1", "r1", "m1");
            startContainerWithTopology("clustered-topology-2", "node1", 100, "s1", "r1", "m2");
            startContainerWithTopology("clustered-topology-3", "node2", 200, "s1", "r2", "m3");

            verifyTopologyHinting("clustered-topology-1", "clustered-topology-2", "clustered-topology-3", "clustered", DEFAULT_CACHE_NAME);
        } finally {
            stopContainers(controller, "clustered-topology-1", "clustered-topology-2", "clustered-topology-3");
        }
    }

    @Test
    public void testTopologyConfigSiteAttribute() throws Exception {
        try {
            startContainerWithTopology("clustered-topology-1", "node0", 0, "s1", "r1", "m1");
            startContainerWithTopology("clustered-topology-2", "node1", 100, "s1", "r2", "m2");
            startContainerWithTopology("clustered-topology-3", "node2", 200, "s2", "r3", "m3");

            verifyTopologyHinting("clustered-topology-1", "clustered-topology-2", "clustered-topology-3", "clustered", DEFAULT_CACHE_NAME);
        } finally {
            stopContainers(controller, "clustered-topology-1", "clustered-topology-2", "clustered-topology-3");
        }
    }

    private void verifyTopologyHinting(String container1, String container2, String container3, String manager, String cache) {
        RemoteInfinispanMBeans s1 = createRemotes(container1, manager, cache);
        RemoteInfinispanMBeans s2 = createRemotes(container2, manager, cache);
        RemoteInfinispanMBeans s3 = createRemotes(container3, manager, cache);

        RemoteCache<Object, Object> s1Cache = createCache(s1);
        RemoteCache<Object, Object> s2Cache = createCache(s2);
        RemoteCache<Object, Object> s3Cache = createCache(s3);

        assertEquals(3, s1.manager.getClusterSize());
        assertEquals(3, s2.manager.getClusterSize());
        assertEquals(3, s3.manager.getClusterSize());
        int total_elements = 0;
        s1Cache.clear();
        s2Cache.clear();
        s3Cache.clear();

        long s0Entries = 0;
        long s1Entries = 0;
        long s2Entries = 0;
        List<String> s1Bulk = new ArrayList<>();
        List<String> s2Bulk = new ArrayList<>();

        // By using topology information we divide our 3 nodes into 2 groups and generate enough elements so there
        // is at least 1 element in each group and at least 5 elements total,
        // and keep track of elements that went to server 2 and 3
        while (s0Entries == 0 || s1Entries == 0 || s2Entries == 0 || total_elements < 5) {
            s1Cache.put("machine" + total_elements, "machine");

            if (s1Entries + 1 == s2.cache.getNumberOfEntries()) {
                s1Bulk.add("machine" + total_elements);
            }
            if (s2Entries + 1 == s3.cache.getNumberOfEntries()) {
                s2Bulk.add("machine" + total_elements);
            }

            total_elements++;
            s1Entries = s2.cache.getNumberOfEntries();
            s2Entries = s3.cache.getNumberOfEntries();
            s0Entries = s1.cache.getNumberOfEntries();
            if (total_elements > 10)
                break; // in case something goes wrong - do not cycle forever
        }

        assertTrue("Unexpected number of entries in server1: " + s0Entries, s0Entries > 0);
        assertTrue("Unexpected number of entries in server2: " + s1Entries, s1Entries > 0);
        assertTrue("Instead of " + total_elements * 2 + " total elements there were " + (s0Entries + s1Entries + s2Entries),
                s0Entries + s1Entries + s2Entries == total_elements * 2);
        assertTrue("Server 1 elements are not contained in server 2", s2Bulk.containsAll(s1Bulk));

        // Now we remove the keys from server 2 therefore they should be removed from server 3 and that should imply
        // that server 3 and server 1 have the same elements
        for (String key : s1Bulk) {
            s2Cache.remove(key);
        }
        s0Entries = s1.cache.getNumberOfEntries();
        s1Entries = s2.cache.getNumberOfEntries();
        s2Entries = s3.cache.getNumberOfEntries();

        assertEquals("There were " + s1Entries + " left in the 2nd server", 0, s1Entries);
        assertEquals(s0Entries, s2Entries);
        assertNotEquals(s0Entries, s1Entries);
        assertEquals(s1Cache.getBulk(), s3Cache.getBulk());
    }

    @Test
    @WithRunningServer({@RunningServer(name = "clustered-1"),@RunningServer(name = "clustered-2")})
    public void testTwoNodesConfig() throws Exception {
        final RemoteInfinispanMBeans s1 = createRemotes("clustered-1", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s2 = createRemotes("clustered-2", "clustered", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> s1Cache = createCache(s1);
        RemoteCache<Object, Object> s2Cache = createCache(s2);
        RESTHelper rest = new RESTHelper();

        rest = rest.withServers(s1.server, s2.server);
        cleanRESTServer(rest);
        assertEquals(0, s1.cache.getNumberOfEntries());
        assertEquals(0, s2.cache.getNumberOfEntries());
        eventually(() -> s1.manager.getClusterSize() == 2, 30000, 300);
        Assert.assertEquals(2, s1.manager.getClusterSize());
        Assert.assertEquals(2, s2.manager.getClusterSize());
        s1Cache.put("k", "v");
        s1Cache.put("k2", "v2");
        assertEquals(s1Cache.get("k"), "v");
        assertEquals(s1Cache.get("k2"), "v2");
        assertEquals(2, s1.cache.getNumberOfEntries());
        s2Cache.put("k3", "v3");
        assertEquals(3, s2.cache.getNumberOfEntries());
        assertEquals("v", s1Cache.get("k"));
        assertEquals("v", s2Cache.get("k"));
        assertEquals("v2", s1Cache.get("k2"));
        assertEquals("v2", s2Cache.get("k2"));
        assertEquals("v3", s1Cache.get("k3"));
        assertEquals("v3", s2Cache.get("k3"));
        cleanRESTServer(rest);
        rest.put(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(1, KEY_A), "data");
        cleanRESTServer(rest);
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(1, KEY_A), "data");
        cleanRESTServer(rest);
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(1, KEY_A), "data");
        rest.delete(rest.fullPathKey(0, KEY_A));
        rest.head(rest.fullPathKey(1, KEY_A), HttpStatus.SC_NOT_FOUND);
        cleanRESTServer(rest);
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.post(rest.fullPathKey(0, KEY_B), "data", "text/plain");
        rest.head(rest.fullPathKey(0, KEY_A));
        rest.head(rest.fullPathKey(0, KEY_B));
        rest.delete(rest.fullPathKey(0, null));
        rest.head(rest.fullPathKey(1, KEY_A), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(1, KEY_B), HttpStatus.SC_NOT_FOUND);
        cleanRESTServer(rest);
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain", HttpStatus.SC_OK,
                // headers
                "Content-Type", "text/plain", "timeToLiveSeconds", "2");
        rest.head(rest.fullPathKey(1, KEY_A));
        sleepForSecs(2.1);
        // should be evicted
        rest.head(rest.fullPathKey(1, KEY_A), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    @WithRunningServer({@RunningServer(name = "clustered-xsite-1"),@RunningServer(name = "clustered-xsite-2"),@RunningServer(name = "clustered-xsite-3")})
    public void testXsiteConfig() throws Exception {
        RemoteInfinispanMBeans s1 = createRemotes("clustered-xsite-1", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s2 = createRemotes("clustered-xsite-2", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s3 = createRemotes("clustered-xsite-3", "clustered", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> s1Cache = createCache(s1);
        RemoteCache<Object, Object> s2Cache = createCache(s2);
        RemoteCache<Object, Object> s3Cache = createCache(s3);
        assertEquals(0, s1.cache.getNumberOfEntries());
        assertEquals(0, s2.cache.getNumberOfEntries());

        assertEquals(2, s1.manager.getClusterSize());
        assertEquals(2, s2.manager.getClusterSize());
        assertEquals(1, s3.manager.getClusterSize());

        s1Cache.put("k1", "v1");
        s1Cache.put("k2", "v2");
        assertEquals(2, s1.cache.getNumberOfEntries());
        assertEquals(2, s2.cache.getNumberOfEntries());
        assertEquals(2, s3.cache.getNumberOfEntries());

        assertEquals(s1Cache.get("k1"), "v1");
        assertEquals(s2Cache.get("k1"), "v1");
        assertEquals(s3Cache.get("k1"), "v1");
        assertEquals(s1Cache.get("k2"), "v2");
        assertEquals(s2Cache.get("k2"), "v2");
        assertEquals(s3Cache.get("k2"), "v2");
    }

    private static void cleanRESTServer(RESTHelper rest) throws Exception {
        rest.delete(rest.fullPathKey(KEY_A));
        rest.delete(rest.fullPathKey(KEY_B));
        rest.delete(rest.fullPathKey(KEY_C));

        rest.head(rest.fullPathKey(KEY_A), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_B), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_C), HttpStatus.SC_NOT_FOUND);
    }

    private ConfigurationBuilder securityConfig(final String keystoreName, final String truststoreName,
                                                RemoteInfinispanServer server) {
        ConfigurationBuilder builder = ITestUtils.createConfigBuilder(server.getHotrodEndpoint().getInetAddress().getHostName(),
                server.getHotrodEndpoint().getPort());
        builder.security().ssl().enable().keyStoreFileName(ITestUtils.SERVER_CONFIG_DIR + File.separator + keystoreName)
                .keyStorePassword("secret".toCharArray())
                .trustStoreFileName(ITestUtils.SERVER_CONFIG_DIR + File.separator + truststoreName)
                .trustStorePassword("secret".toCharArray()).maxRetries(3);
        return builder;
    }

    private void doPutGetRemove(final RemoteInfinispanMBeans sMain, final RemoteInfinispanMBeans sRemoteStore) {
        assertEquals(0, sMain.cache.getNumberOfEntries());
        assertEquals(0, sRemoteStore.cache.getNumberOfEntries());
        RemoteCache<Object, Object> sMainCache = createCache(sMain);
        RemoteCache<Object, Object> sRemoteStoreCache = createCache(sRemoteStore);

        for (int i = 0; i < 1100; i++) {
            sMainCache.put("key" + i, "value" + i);
        }
        assertTrue(sMain.cache.getNumberOfEntriesInMemory() <= 1000);
        eventually(() -> {
            log.debug("Num entries: Main cache: " + sMain.cache.getNumberOfEntries() + " Remote store: "
                  + sRemoteStore.cache.getNumberOfEntriesInMemory() + " Total: "
                  + (sMain.cache.getNumberOfEntriesInMemory() + sRemoteStore.cache.getNumberOfEntriesInMemory()));
            return sMain.cache.getNumberOfEntriesInMemory() + sRemoteStore.cache.getNumberOfEntriesInMemory() == 1100;
        }, 10000);

        for (int i = 0; i < 1100; i++) {
            assertNotNull(sMainCache.get("key" + i));
            sMainCache.remove("key" + i);
            assertNull(sMainCache.get("key" + i));
        }
        assertEquals(0, sMain.cache.getNumberOfEntries());
        assertEquals(0, sRemoteStore.cache.getNumberOfEntries());
        sMainCache.clear();
        sRemoteStoreCache.clear();
    }

    private void doPutGetWithExpiration(RemoteInfinispanMBeans s1, RemoteInfinispanMBeans s2) {
        assertEquals(0, s2.cache.getNumberOfEntriesInMemory());
        RemoteCache<Object, Object> s1Cache = createCache(s1);
        doPutGet(s1, s1Cache);
        // all entries are in store (passivation=false)
        assertEquals(10, s2.cache.getNumberOfEntriesInMemory());

        sleepForSecs(3.1); // the lifespan is 3000ms so we need to wait more

        // entries expired
        for (int i = 0; i < 10; i++) {
            assertNull(s1Cache.get("key" + i));
        }
        assertEquals(0, s2.cache.getNumberOfEntriesInMemory());
    }

    private void doPutGet(RemoteInfinispanMBeans s, RemoteCache<Object, Object> c) {
        assertEquals(0, s.cache.getNumberOfEntriesInMemory());
        for (int i = 0; i < 10; i++) {
            c.put("k" + i, "v" + i);
        }
        for (int i = 0; i < 10; i++) {
            assertEquals("v" + i, c.get("k" + i));
        }
        assertEquals(10, s.cache.getNumberOfEntriesInMemory());
    }

    private void doPutGetCheckPath(RemoteInfinispanMBeans s, String filePath, double sleepTime) {
        RemoteCache<Object, Object> sCache = createCache(s);
        doPutGet(s, sCache);
        if (sleepTime >= 0) {
            sleepForSecs(sleepTime);

            // entries expired
            for (int i = 0; i < 10; i++) {
                assertNull(sCache.get("k" + i));
            }
        }
        File f = new File(ITestUtils.SERVER_DATA_DIR, filePath);
        assertTrue(f.isDirectory());
    }

    /*
     * Need to de-serialize the object as the default JavaSerializationMarshaller is used by Memcached endpoint.
     */
    private byte[] readWithMemcachedAndDeserialize(String key, MemcachedClient memcachedClient) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(memcachedClient.getBytes(key));
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (byte[]) ois.readObject();
    }

    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans, ConfigurationBuilder configBuilder) {
        return rcmFactory.createCache(configBuilder, cacheBeans.cacheName);
    }

    protected RemoteCache<Object, Object> createCache(RemoteInfinispanMBeans cacheBeans) {
        return rcmFactory.createCache(cacheBeans);
    }

    protected RemoteInfinispanMBeans createRemotes(String serverName, String managerName, String cacheName) {
        return RemoteInfinispanMBeans.create(serverManager, serverName, cacheName, managerName);
    }

    private void startContainerWithTopology(String containerName, String nodeName, int portOffset, String site, String rack, String machine) {
        controller.start(containerName, new Config().add("javaVmArguments", System.getProperty("server.jvm.args")
                + " -Djboss.node.name=" + nodeName
                + " -Djboss.socket.binding.port-offset=" + portOffset
                + " -Djboss.jgroups.topology.site=" + site
                + " -Djboss.jgroups.topology.rack=" + rack
                + " -Djboss.jgroups.topology.machine=" + machine
        ).map());
    }
}
