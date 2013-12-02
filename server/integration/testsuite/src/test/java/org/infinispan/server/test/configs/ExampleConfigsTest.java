/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.test.configs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RESTEndpoint;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.server.test.client.memcached.MemcachedClient;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
import org.infinispan.server.test.util.TestUtil;
import org.infinispan.server.test.util.TestUtil.Condition;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.client.rest.RESTHelper.*;
import static org.infinispan.server.test.util.TestUtil.eventually;
import static org.infinispan.server.test.util.TestUtil.invokeOperation;
import static org.junit.Assert.*;

/**
 * Tests for example configurations.
 *
 * @author Jakub Markos (jmarkos@redhat.com)
 * @author Galder Zamarreño (galderz@redhat.com)
 * @author Michal Linhard (mlinhard@redhat.com)
 */
@RunWith(Arquillian.class)
public class ExampleConfigsTest {

    private static final Logger log = Logger.getLogger(ExampleConfigsTest.class);

    @InfinispanResource
    RemoteInfinispanServers serverManager;

    static final String DEFAULT_CACHE_NAME = "default";
    static final String NAMED_CACHE_NAME = "namedCache";
    static final String MEMCACHED_CACHE_NAME = "memcachedCache";

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

    /**
     * Create a 2 node cluster and check that state transfer does not take place.
     *
     */
    @Test
    public void testClusterCacheLoaderConfigExample() throws Exception {
        controller.start("clustered-ccl-1");
        try {
            RemoteInfinispanMBeans s1 = createRemotes("clustered-ccl-1", "clustered", DEFAULT_CACHE_NAME);
            RemoteCache<Object, Object> s1Cache = createCache(s1);
            s1Cache.put("key", "value");
            assertEquals(1, s1.cache.getNumberOfEntries());
            assertEquals(1, s1.manager.getClusterSize());

            controller.start("clustered-ccl-2");
            RemoteInfinispanMBeans s2 = createRemotes("clustered-ccl-2", "clustered", DEFAULT_CACHE_NAME);
            RemoteCache<Object, Object> s2Cache = createCache(s2);

            assertEquals(2, s2.manager.getClusterSize());
            // state transfer didn't happen
            assertEquals(0, s2.cache.getNumberOfEntries());
            s2Cache.get("key");
            // the entry is obtained
            assertEquals(1, s2.cache.getNumberOfEntries());
            s2Cache.put("key2", "value2");
            assertEquals(2, s1.cache.getNumberOfEntries());
            assertEquals(2, s2.cache.getNumberOfEntries());
        } finally {
            if (controller.isStarted("clustered-ccl-1")) {
                controller.stop("clustered-ccl-1");
            }
            if (controller.isStarted("clustered-ccl-2")) {
                controller.stop("clustered-ccl-2");
            }
        }
    }

    @Test
    public void testHotRodRollingUpgrades() throws Exception {
        // Target node
        final int managementPortServer1 = 9999;
        MBeanServerConnectionProvider provider1;
        // Source node
        final int managementPortServer2 = 10099;
        MBeanServerConnectionProvider provider2;

        controller.start("hotrod-rolling-upgrade-2");
        try {
            RemoteInfinispanMBeans s2 = createRemotes("hotrod-rolling-upgrade-2", "local", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c2 = createCache(s2);

            c2.put("key1", "value1");
            assertEquals("value1", c2.get("key1"));

            for (int i = 0; i < 50; i++) {
                c2.put("keyLoad" + i, "valueLoad" + i);
            }

            controller.start("hotrod-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("hotrod-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c1 = createCache(s1);

            assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", c1.get("key1"));

            provider1 = new MBeanServerConnectionProvider(s1.server.getHotrodEndpoint().getInetAddress().getHostName(),
                managementPortServer1);
            provider2 = new MBeanServerConnectionProvider(s2.server.getHotrodEndpoint().getInetAddress().getHostName(),
                managementPortServer2);

            final ObjectName rollMan = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(local)\","
                + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider2, rollMan.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

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
        } finally {
            if (controller.isStarted("hotrod-rolling-upgrade-1")) {
                controller.stop("hotrod-rolling-upgrade-1");
            }
            if (controller.isStarted("hotrod-rolling-upgrade-2")) {
                controller.stop("hotrod-rolling-upgrade-2");
            }
        }
    }

    @Test
    public void testRestRollingUpgrades() throws Exception {
        // target node
        final int managementPortServer1 = 9999;
        MBeanServerConnectionProvider provider1;
        // Source node
        final int managementPortServer2 = 10099;
        MBeanServerConnectionProvider provider2;

        controller.start("rest-rolling-upgrade-2");
        try {
            RemoteInfinispanMBeans s2 = createRemotes("rest-rolling-upgrade-2", "local", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c2 = createCache(s2);

            c2.put("key1", "value1");
            assertEquals("value1", c2.get("key1"));

            for (int i = 0; i < 50; i++) {
                c2.put("keyLoad" + i, "valueLoad" + i);
            }

            controller.start("rest-rolling-upgrade-1");

            RemoteInfinispanMBeans s1 = createRemotes("rest-rolling-upgrade-1", "local", DEFAULT_CACHE_NAME);
            final RemoteCache<Object, Object> c1 = createCache(s1);

            assertEquals("Can't access etries stored in source node (target's RestCacheStore).", "value1", c1.get("key1"));

            provider1 = new MBeanServerConnectionProvider(s1.server.getRESTEndpoint().getInetAddress().getHostName(),
                managementPortServer1);
            provider2 = new MBeanServerConnectionProvider(s2.server.getRESTEndpoint().getInetAddress().getHostName(),
                managementPortServer2);

            final ObjectName rollMan = new ObjectName("jboss.infinispan:type=Cache," + "name=\"default(local)\","
                + "manager=\"local\"," + "component=RollingUpgradeManager");

            invokeOperation(provider2, rollMan.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

            invokeOperation(provider1, rollMan.toString(), "synchronizeData", new Object[]{"rest"},
                    new String[]{"java.lang.String"});

            invokeOperation(provider1, rollMan.toString(), "disconnectSource", new Object[]{"rest"},
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
    @WithRunningServer("standalone-compatibility-mode")
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
            HttpResponse getResponse = restClient.execute(get);
            assertEquals(HttpServletResponse.SC_OK, getResponse.getStatusLine().getStatusCode());
            assertArrayEquals("v1".getBytes(), EntityUtils.toByteArray(getResponse.getEntity()));

            // 3. Get with Memcached
            assertArrayEquals("v1".getBytes(), readWithMemcachedAndDeserialize(key, memcachedClient));

            key = "2";

            // 1. Put with REST
            HttpPut put = new HttpPut(restUrl + "/" + key);
            put.setEntity(new ByteArrayEntity("<hey>ho</hey>".getBytes(), ContentType.APPLICATION_OCTET_STREAM));
            HttpResponse putResponse = restClient.execute(put);
            assertEquals(HttpServletResponse.SC_OK, putResponse.getStatusLine().getStatusCode());

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
    @WithRunningServer("standalone-fcs-local")
    public void testFileCacheStoreConfig() throws Exception {
        doPutGetCheckPath(createRemotes("standalone-fcs-local", "local", DEFAULT_CACHE_NAME), "dc", -1);
        doPutGetCheckPath(createRemotes("standalone-fcs-local", "local", MEMCACHED_CACHE_NAME), "mc", -1);
        doPutGetCheckPath(createRemotes("standalone-fcs-local", "local", NAMED_CACHE_NAME), "nc", 2100);
    }

    @Test
    @WithRunningServer("clustered-jdbc")
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
    @WithRunningServer("standalone-leveldb-cs-local")
    public void testLevelDBCacheStoreConfig() throws Exception {
        doPutGetCheckPath(createRemotes("standalone-leveldb-cs-local", "local", DEFAULT_CACHE_NAME), "level-dcdefault", -1);
        doPutGetCheckPath(createRemotes("standalone-leveldb-cs-local", "local", MEMCACHED_CACHE_NAME),
            "level-mcmemcachedCache", -1);
        doPutGetCheckPath(createRemotes("standalone-leveldb-cs-local", "local", NAMED_CACHE_NAME), "leveldb-ncnamedCache", 2100);
    }

    @Test
    @WithRunningServer("standalone-hotrod-multiple")
    public void testHotrodMultipleConfig() throws Exception {
        RemoteInfinispanMBeans s = createRemotes("standalone-hotrod-multiple", "local", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> c1 = createCache(s, TestUtil.createConfigBuilder(s.server.getHotrodEndpoint("external")
            .getInetAddress().getHostName(), s.server.getHotrodEndpoint("external").getPort()));
        RemoteCache<Object, Object> c2 = createCache(s, TestUtil.createConfigBuilder(s.server.getHotrodEndpoint("internal")
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
    @WithRunningServer({ "standalone-rcs-local-2", "standalone-rcs-local-1" })
    public void testRemoteCacheStoreConfig() throws Exception {
        RemoteInfinispanMBeans sRemoteStoreDefault = createRemotes("standalone-rcs-local-2", "local", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans sRemoteStoreMemcached = createRemotes("standalone-rcs-local-2", "local", MEMCACHED_CACHE_NAME);
        RemoteInfinispanMBeans sRemoteStoreNamed = createRemotes("standalone-rcs-local-2", "local", NAMED_CACHE_NAME);
        RemoteInfinispanMBeans s1Default = createRemotes("standalone-rcs-local-1", "local", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s1Memcached = createRemotes("standalone-rcs-local-1", "local", MEMCACHED_CACHE_NAME);
        RemoteInfinispanMBeans s1Named = createRemotes("standalone-rcs-local-1", "local", NAMED_CACHE_NAME);

        doPutGetRemove(s1Default, sRemoteStoreDefault);
        doPutGetRemove(s1Memcached, sRemoteStoreMemcached);
        doPutGetWithExpiration(s1Named, sRemoteStoreNamed);
    }

    @Test
    @WithRunningServer("standalone-hotrod-ssl")
    public void testSSLHotRodConfig() throws Exception {
        RemoteInfinispanMBeans s = createRemotes("standalone-hotrod-ssl", "local", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> c = createCache(s, securityConfig("keystore_client.jks", "truststore_client.jks", s.server));
        doPutGet(s, c);
        try {
            doPutGet(s, createCache(s, securityConfig("keystore_server.jks", "truststore_client.jks", s.server)));
            Assert.fail();
        } catch (TransportException e) {
            // ok
        }
        try {
            doPutGet(s, createCache(s, securityConfig("keystore_client.jks", "truststore_server.jks", s.server)));
            Assert.fail();
        } catch (TransportException e) {
            // ok
        }
    }

    @Test
    @WithRunningServer({ "clustered-storage-only-1", "clustered-storage-only-2" })
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
            TestUtil.createCacheManager(serverManager.getServer("clustered-storage-only-2"));
            Assert.fail();
        } catch (Exception e) {
            // OK - we are not able to access HotRod endpoint of storage-only node
        }
    }

    @Test
    @WithRunningServer({ "clustered-topology-1", "clustered-topology-2", "clustered-topology-3" })
    public void testTopologyConfig() throws Exception {
        RemoteInfinispanMBeans s1 = createRemotes("clustered-topology-1", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s2 = createRemotes("clustered-topology-2", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s3 = createRemotes("clustered-topology-3", "clustered", DEFAULT_CACHE_NAME);
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
        List<String> s1Bulk = new ArrayList<String>();
        List<String> s2Bulk = new ArrayList<String>();

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
    @WithRunningServer({ "clustered-two-nodes-1", "clustered-two-nodes-2" })
    public void testTwoNodesConfig() throws Exception {
        final RemoteInfinispanMBeans s1 = createRemotes("clustered-two-nodes-1", "clustered", DEFAULT_CACHE_NAME);
        RemoteInfinispanMBeans s2 = createRemotes("clustered-two-nodes-2", "clustered", DEFAULT_CACHE_NAME);
        RemoteCache<Object, Object> s1Cache = createCache(s1);
        RemoteCache<Object, Object> s2Cache = createCache(s2);
        addServer(s1.server);
        addServer(s2.server);
        setUpREST(s1.server, s2.server);
        assertEquals(0, s1.cache.getNumberOfEntries());
        assertEquals(0, s2.cache.getNumberOfEntries());
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return s1.manager.getClusterSize() == 2;
            }
        }, 30000, 300);
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
        setUpREST(s1.server, s2.server);
        put(fullPathKey(0, KEY_A), "data", "text/plain");
        get(fullPathKey(1, KEY_A), "data");
        setUpREST(s1.server, s2.server);
        post(fullPathKey(0, KEY_A), "data", "text/plain");
        get(fullPathKey(1, KEY_A), "data");
        setUpREST(s1.server, s2.server);
        post(fullPathKey(0, KEY_A), "data", "text/plain");
        get(fullPathKey(1, KEY_A), "data");
        delete(fullPathKey(0, KEY_A));
        head(fullPathKey(1, KEY_A), HttpServletResponse.SC_NOT_FOUND);
        setUpREST(s1.server, s2.server);
        post(fullPathKey(0, KEY_A), "data", "text/plain");
        post(fullPathKey(0, KEY_B), "data", "text/plain");
        head(fullPathKey(0, KEY_A));
        head(fullPathKey(0, KEY_B));
        delete(fullPathKey(0, null));
        head(fullPathKey(1, KEY_A), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(1, KEY_B), HttpServletResponse.SC_NOT_FOUND);
        setUpREST(s1.server, s2.server);
        post(fullPathKey(0, KEY_A), "data", "application/text", HttpServletResponse.SC_OK,
        // headers
            "Content-Type", "application/text", "timeToLiveSeconds", "2");
        head(fullPathKey(1, KEY_A));
        Thread.sleep(2100);
        // should be evicted
        head(fullPathKey(1, KEY_A), HttpServletResponse.SC_NOT_FOUND);
    }

    @Test
    @WithRunningServer({ "clustered-xsite-1", "clustered-xsite-2", "clustered-xsite-3" })
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

    private void setUpREST(RemoteInfinispanServer server1, RemoteInfinispanServer server2) throws Exception {
        delete(fullPathKey(KEY_A));
        delete(fullPathKey(KEY_B));
        delete(fullPathKey(KEY_C));
        delete(fullPathKey(NAMED_CACHE_NAME, KEY_A));

        head(fullPathKey(KEY_A), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(KEY_B), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(KEY_C), HttpServletResponse.SC_NOT_FOUND);
        head(fullPathKey(NAMED_CACHE_NAME, KEY_A), HttpServletResponse.SC_NOT_FOUND);
    }

    private void addServer(RemoteInfinispanServer server) {
        RESTEndpoint endpoint = server.getRESTEndpoint();
        // IPv6 addresses should be in square brackets, otherwise http client does not understand it
        // otherwise should be IPv4
        String inetHostName = endpoint.getInetAddress().getHostName();
        String realHostName = endpoint.getInetAddress() instanceof Inet6Address ? "[" + inetHostName + "]" : inetHostName;
        RESTHelper.addServer(realHostName, endpoint.getContextPath());
    }

    private ConfigurationBuilder securityConfig(final String keystoreName, final String truststoreName,
        RemoteInfinispanServer server) {
        ConfigurationBuilder builder = TestUtil.createConfigBuilder(server.getHotrodEndpoint().getInetAddress().getHostName(),
            server.getHotrodEndpoint().getPort());
        builder.ssl().enable().keyStoreFileName(TestUtil.SERVER_CONFIG_DIR + File.separator + keystoreName)
            .keyStorePassword("secret".toCharArray())
            .trustStoreFileName(TestUtil.SERVER_CONFIG_DIR + File.separator + truststoreName)
            .trustStorePassword("secret".toCharArray());
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
        assertTrue(sMain.cache.getNumberOfEntries() <= 1000);
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                log.debug("Num entries: Main cache: " + sMain.cache.getNumberOfEntries() + " Remote store: "
                    + sRemoteStore.cache.getNumberOfEntries() + " Total: "
                    + (sMain.cache.getNumberOfEntries() + sRemoteStore.cache.getNumberOfEntries()));
                return sMain.cache.getNumberOfEntries() + sRemoteStore.cache.getNumberOfEntries() == 1100;
            }
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

    private void doPutGetWithExpiration(RemoteInfinispanMBeans s1, RemoteInfinispanMBeans s2) throws Exception {
        assertEquals(0, s2.cache.getNumberOfEntries());
        RemoteCache<Object, Object> s1Cache = createCache(s1);
        doPutGet(s1, s1Cache);
        // all entries are in store (passivation=false)
        assertEquals(10, s2.cache.getNumberOfEntries());

        Thread.sleep(2100); // the lifespan is 2000ms so we need to wait more

        // entries expired
        for (int i = 0; i < 10; i++) {
            assertNull(s1Cache.get("key" + i));
        }
    }

    private void doPutGet(RemoteInfinispanMBeans s, RemoteCache<Object, Object> c) {
        assertEquals(0, s.cache.getNumberOfEntries());
        for (int i = 0; i < 10; i++) {
            c.put("k" + i, "v" + i);
        }
        for (int i = 0; i < 10; i++) {
            assertEquals("v" + i, c.get("k" + i));
        }
        assertEquals(10, s.cache.getNumberOfEntries());
    }

    private void doPutGetCheckPath(RemoteInfinispanMBeans s, String filePath, long sleepTime) throws Exception {
        RemoteCache<Object, Object> sCache = createCache(s);
        doPutGet(s, sCache);
        if (sleepTime >= 0) {
            Thread.sleep(sleepTime);

            // entries expired
            for (int i = 0; i < 10; i++) {
                assertNull(sCache.get("k" + i));
            }
        }
        File f = new File(TestUtil.SERVER_DATA_DIR, filePath);
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

    protected RemoteCacheManager createCacheManager(RemoteInfinispanMBeans cacheBeans) {
        return rcmFactory.createManager(cacheBeans);
    }

    protected RemoteInfinispanMBeans createRemotes(String serverName, String managerName, String cacheName) {
        return RemoteInfinispanMBeans.create(serverManager, serverName, cacheName, managerName);
    }
}
