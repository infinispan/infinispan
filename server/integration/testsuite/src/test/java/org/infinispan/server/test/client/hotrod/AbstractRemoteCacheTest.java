package org.infinispan.server.test.client.hotrod;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for HotRod client and its RemoteCache API. Subclasses must provide a way to get the list of remote HotRod
 * servers and to assert the cache is empty.
 * <p/>
 * Subclasses may be used in Client-Server mode or Hybrid mode where HotRod server runs as a library deployed in an
 * application server.
 *
 * @author Richard Achmatowicz
 * @author Martin Gencur
 * @author Jozef Vilkolak
 */
public abstract class AbstractRemoteCacheTest {

    private final String TEST_CACHE_NAME = "testcache";

    protected RemoteCache remoteCache;
    protected static RemoteCacheManager remoteCacheManager = null;
    protected final int ASYNC_OPS_ENTRY_LOAD = 100;

    protected abstract List<RemoteInfinispanServer> getServers();

    @Before
    public void initialize() {
        if (remoteCacheManager == null) {
            Configuration config = createRemoteCacheManagerConfiguration();
            remoteCacheManager = new RemoteCacheManager(config, true);
        }
        remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
        assertCacheEmpty();
    }

    @AfterClass
    public static void release() {
        if (remoteCacheManager != null) {
            remoteCacheManager.stop();
        }
    }

    private Configuration createRemoteCacheManagerConfiguration() {
        ConfigurationBuilder config = new ConfigurationBuilder();
        for (RemoteInfinispanServer server : getServers()) {
            config.addServer().host(server.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(server.getHotrodEndpoint().getPort());
        }
        config.balancingStrategy("org.infinispan.server.test.client.hotrod.HotRodTestRequestBalancingStrategy")
                // load balancing
                .balancingStrategy("org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy")
                        // list of HotRod servers available to connect to
                        //.addServers(hotRodServerList)
                .forceReturnValues(false)
                        // TCP stuff
                .tcpNoDelay(true)
                .pingOnStartup(true)
                .transportFactory("org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory")
                        // marshalling
                .marshaller("org.infinispan.commons.marshall.jboss.GenericJBossMarshaller")
                        // executors
                .asyncExecutorFactory().factoryClass("org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.pool_size", "10")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.queue_size", "100000")
                        //hashing
                .keySizeEstimate(64)
                .valueSizeEstimate(512);

        if (isDistributedMode()) {
            config.consistentHashImpl(1, "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1");
        } else {
            config.consistentHashImpl(2, "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2");
        }

        return config.build();
    }

    private boolean isDistributedMode() {
        return "dist".equals(AbstractRemoteCacheManagerTest.getClusteringMode());
    }

    public void assertCacheEmpty() {
        clearServer(0);
        if (!AbstractRemoteCacheManagerTest.isLocalMode()) {
            clearServer(1);
        }
    }

    private void clearServer(int serverIndex) {
        while (remoteCache.size() != 0) {
            remoteCache.clear();
        }
    }

    private long numEntriesOnServer(int serverIndex) {
        return getServers().get(serverIndex).
                getCacheManager(AbstractRemoteCacheManagerTest.isLocalMode() ? "local" : "clustered").
                getCache(TEST_CACHE_NAME).getNumberOfEntries();
    }

    @Test
    public void testReplaceWithVersionWithLifespan() throws Exception {
        int lifespanInSecs = 1;
        assertNull(remoteCache.replace("aKey", "aValue"));
        remoteCache.put("aKey", "aValue");
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        assertTrue(remoteCache.replaceWithVersion("aKey", "aNewValue", valueBinary.getVersion(), lifespanInSecs));

        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertEquals("aNewValue", entry2.getValue());

        sleepForSecs(lifespanInSecs + 1);
        assertNull(remoteCache.getVersioned("aKey"));
    }

    @Test
    public void testPut() throws IOException {
        assertNull(remoteCache.put("aKey", "aValue"));
        assertTrue(remoteCache.containsKey("aKey"));
        assertEquals(remoteCache.get("aKey"), "aValue");
    }

    @Test
    public void testPutWithLifespan() {
        long lifespanInSecs = 1;
        remoteCache.put("lkey", "value", lifespanInSecs, TimeUnit.SECONDS);
        sleepForSecs(lifespanInSecs + 1);
        assertNull(remoteCache.get("lkey"));
    }

    @Test
    public void testSize() {
        assertEquals(0, remoteCache.size());

        // with force_return_value=false as default, this opertion
        // should return null even if a previous value for aPut was there
        assertNull(remoteCache.put("aKey", "aValue"));
        assertTrue(remoteCache.containsKey("aKey"));
        assertEquals(remoteCache.size(), 1);

        // should be idempotent
        assertEquals(remoteCache.size(), 1);

        assertNull(remoteCache.put("anotherKey", "anotherValue"));
        assertTrue(remoteCache.containsKey("anotherKey"));
        assertEquals(remoteCache.size(), 2);

        assertNull(remoteCache.remove("anotherKey"));
        assertTrue(!remoteCache.containsKey("anotherKey"));
        assertEquals(remoteCache.size(), 1);

        assertNull(remoteCache.remove("aKey"));
        assertTrue(!remoteCache.containsKey("aKey"));
        assertEquals(remoteCache.size(), 0);
    }

    @Test
    public void testIsEmpty() throws IOException {

        assertTrue(remoteCache.isEmpty());

        assertNull(remoteCache.put("aKey", "aValue"));
        assertTrue(remoteCache.containsKey("aKey"));
        assertTrue(!remoteCache.isEmpty());

        assertNull(remoteCache.remove("aKey"));
        assertTrue(!remoteCache.containsKey("aKey"));
        assertTrue(remoteCache.isEmpty());
    }

    @Test
    public void testContains() {
        assertTrue(!remoteCache.containsKey("aKey"));
        remoteCache.put("aKey", "aValue");
        assertTrue(remoteCache.containsKey("aKey"));
    }

    @Test
    public void testWithFlags() throws IOException {

        assertNull(remoteCache.put("aKey", "aValue"));
        assertTrue(remoteCache.containsKey("aKey"));
        assertEquals("aValue", remoteCache.get("aKey"));

        // should not return return old value
        assertNull(remoteCache.put("aKey", "anotherValue"));
        assertEquals("anotherValue", remoteCache.get("aKey"));

        // now should return old value
        assertEquals("anotherValue", remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).put("aKey", "yetAnotherValue"));
    }

    @Test
    public void testBulkOperations() {

        Map<String, String> mapIn;
        Map<String, String> mapOut = new HashMap<String, String>();
        mapOut.put("aKey", "aValue");
        mapOut.put("bKey", "bValue");
        mapOut.put("cKey", "cValue");

        remoteCache.putAll(mapOut);
        mapIn = remoteCache.getBulk();

        // check that the maps are equal
        assertEquals(mapIn, mapOut);
    }

    @Test
    public void testBulkOperationsWithLifespan() {

        long lifespanInSecs = 1;

        Map<String, String> mapIn = new HashMap<String, String>();
        Map<String, String> mapOut = new HashMap<String, String>();
        mapOut.put("aKey", "aValue");
        mapOut.put("bKey", "bValue");
        mapOut.put("cKey", "cValue");

        remoteCache.putAll(mapOut, lifespanInSecs, TimeUnit.SECONDS);

        // give the elements time to be evicted
        sleepForSecs(lifespanInSecs + 1);

        mapIn = remoteCache.getBulk();
        assertEquals(mapIn.size(), 0);
    }

    @Test
    public void testGetBulkWithLimit() {
        Map<String, String> mapIn;
        Map<String, String> mapOut = new HashMap<String, String>();
        mapOut.put("aKey", "aValue");
        mapOut.put("bKey", "bValue");
        mapOut.put("cKey", "cValue");

        remoteCache.putAll(mapOut);
        mapIn = remoteCache.getBulk(2);
        // we don't know which 2 entries will be retrieved
        assertEquals(mapIn.size(), 2);
    }

    @Test
    public void testGetName() {
        // in hotrod protocol specification, the default cache is identified by an empty string
        assertEquals(TEST_CACHE_NAME, remoteCache.getName());
    }

    @Test
    public void testKeySet() {
        remoteCache.put("k1", "v1");
        remoteCache.put("k2", "v2");
        remoteCache.put("k3", "v3");

        Set<String> expectedKeySet = new HashSet<String>();
        expectedKeySet.add("k1");
        expectedKeySet.add("k2");
        expectedKeySet.add("k3");
        assertEquals(expectedKeySet, remoteCache.keySet());
    }

    @Test
    public void testGetWithMetadata() {
        remoteCache.put("k1", "v1", 10000000, TimeUnit.MICROSECONDS); // setting only lifespan
        remoteCache.put("k2", "v2", 10, TimeUnit.SECONDS, 10, TimeUnit.SECONDS); // lifespan + maxIdleTime
        MetadataValue<String> k1 = remoteCache.getWithMetadata("k1");
        MetadataValue<String> k2 = remoteCache.getWithMetadata("k2");
        assertEquals(k1.getValue(), "v1");
        // microseconds converted to seconds
        assertEquals(k1.getLifespan(), 10);
        assertEquals(k1.getMaxIdle(), -1);
        assertEquals(k2.getValue(), "v2");
        assertEquals(k2.getLifespan(), 10);
        assertEquals(k2.getMaxIdle(), 10);
    }

    @Test
    public void testRemoveAsync() throws Exception {
        for (int i = 0; i <= ASYNC_OPS_ENTRY_LOAD; i++) {
            remoteCache.put("key" + i, "value" + i);
        }
        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i <= ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.removeAsync("key" + i));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        assertEquals(0, numEntriesOnServer(0));
    }

    @Test
    public void testReplaceAsync() throws Exception {
        remoteCache.clear();
        for (int i = 0; i <= ASYNC_OPS_ENTRY_LOAD; i++) {
            remoteCache.put("key" + i, "value" + i);
        }
        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i <= ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.replaceAsync("key" + i, "newValue" + i, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        for (int i = 0; i <= ASYNC_OPS_ENTRY_LOAD; i++) {
            assertEquals("newValue" + i, remoteCache.get("key" + i));
        }
    }

    @Test
    public void testGetVersionedCacheEntry() {

        VersionedValue value = remoteCache.getVersioned("aKey");
        assertNull("expected null but received: " + value, remoteCache.getVersioned("aKey"));

        remoteCache.put("aKey", "aValue");
        assertEquals("aValue", remoteCache.get("aKey"));
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        assertNotNull(valueBinary);
        assertEquals(valueBinary.getValue(), "aValue");

        // now put the same value
        remoteCache.put("aKey", "aValue");
        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertEquals(entry2.getValue(), "aValue");

        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertNotEquals(valueBinary, entry2);

        // now put a different value
        remoteCache.put("aKey", "anotherValue");
        VersionedValue entry3 = remoteCache.getVersioned("aKey");
        assertEquals(entry3.getValue(), "anotherValue");
        assertNotEquals(entry3.getVersion(), entry2.getVersion());
        assertNotEquals(entry3, entry2);
    }

    @Test
    public void testReplace() {
        // this should return null, indicating no k/v pair in the map
        assertNull(remoteCache.replace("aKey", "anotherValue"));
        remoteCache.put("aKey", "aValue");
        assertNull(remoteCache.replace("aKey", "anotherValue"));
        assertEquals(remoteCache.get("aKey"), "anotherValue");
    }

    @Test
    public void testReplaceWithVersion() {
        assertNull(remoteCache.replace("aKey", "aValue"));

        remoteCache.put("aKey", "aValue");
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        // replacement should take place (and so return true)
        assertTrue(remoteCache.replaceWithVersion("aKey", "aNewValue", valueBinary.getVersion()));

        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertEquals(entry2.getValue(), "aNewValue");

        // replacement should not take place because we have changed the value
        assertTrue(!remoteCache.replaceWithVersion("aKey", "aNewValue", valueBinary.getVersion()));
    }

    @Test
    public void testRemove() throws IOException {
        assertNull(remoteCache.put("aKey", "aValue"));
        assertEquals(remoteCache.get("aKey"), "aValue");

        assertNull(remoteCache.remove("aKey"));
        assertTrue(!remoteCache.containsKey("aKey"));
    }

    @Test
    public void testRemoveWithVersion() {

        assertTrue(!remoteCache.removeWithVersion("aKey", 12321212l));

        remoteCache.put("aKey", "aValue");
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        assertTrue(remoteCache.removeWithVersion("aKey", valueBinary.getVersion()));

        remoteCache.put("aKey", "aNewValue");

        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertEquals(entry2.getValue(), "aNewValue");

        assertTrue(!remoteCache.removeWithVersion("aKey", valueBinary.getVersion()));
    }

    @Test
    public void testPutIfAbsent() {

        remoteCache.putIfAbsent("aKey", "aValue");
        assertEquals(remoteCache.size(), 1);
        assertEquals(remoteCache.get("aKey"), "aValue");

        assertNull(remoteCache.putIfAbsent("aKey", "anotherValue"));
        assertEquals(remoteCache.get("aKey"), "aValue");
    }

    @Test
    public void testPutIfAbsentWithLifespan() throws Exception {
        int lifespanInSecs = 1;
        remoteCache.putIfAbsent("aKey", "aValue", lifespanInSecs, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
        assertEquals(1, remoteCache.size());
        assertEquals("aValue", remoteCache.get("aKey"));

        sleepForSecs(lifespanInSecs + 1);
        //verify the entry expired
        assertEquals(null, remoteCache.get("akey"));

        remoteCache.putIfAbsent("aKey", "aValue");
        assertEquals(1, remoteCache.size());
        assertEquals("aValue", remoteCache.get("aKey"));
        assertEquals(null, remoteCache.putIfAbsent("aKey", "anotherValue", lifespanInSecs, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));

        sleepForSecs(lifespanInSecs + 1);
        //verify the entry is still there because it was not put with lifespan since it had already existed
        assertEquals("aValue", remoteCache.get("aKey"));
    }

    @Test
    public void testClear() {
        remoteCache.put("aKey", "aValue");
        remoteCache.put("aKey2", "aValue");
        remoteCache.clear();
        assertTrue(!remoteCache.containsKey("aKey"));
        assertTrue(!remoteCache.containsKey("aKey2"));
    }

    @Test
    public void testGetRemoteCacheManager() {

        RemoteCacheManager manager = null;

        manager = remoteCache.getRemoteCacheManager();
        assertEquals("getRemoteCachemanager() returned incorrect value", manager, remoteCacheManager);
    }

    @Test
    public void testStats() {
        ServerStatistics remoteStats = remoteCache.stats();
        assertNotNull(remoteStats);
        System.out.println("named stats = " + remoteStats.getStatsMap());
    }

    @Test
    public void testUnsupportedOperations() {

        try {
            remoteCache.remove("aKey", "aValue");
            fail("call to remove() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.removeAsync("aKey", "aValue");
            fail("call to removeAsync() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }

        try {
            remoteCache.replace("aKey", "oldValue", "newValue");
            fail("call to replace() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.replace("aKey", "oldValue", "newValue", -1, TimeUnit.SECONDS);
            fail("call to replace() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.replace("aKey", "oldValue", "newValue", -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
            fail("call to replace() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.replaceAsync("aKey", "oldValue", "newValue");
            fail("call to replaceAsync() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.replaceAsync("aKey", "oldValue", "newValue", -1, TimeUnit.SECONDS);
            fail("call to replaceAsync() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.replaceAsync("aKey", "oldValue", "newValue", -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS);
            fail("call to replaceAsync() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.containsValue("aValue");
            fail("call to containsValue() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.entrySet();
            fail("call to entrySet() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
        try {
            remoteCache.values();
            fail("call to values() did not raise UnsupportedOperationException ");
        } catch (UnsupportedOperationException uoe) {
            // Unsupported operation exception correctly thrown
        }
    }

    @Test
    public void testClearAsync() throws Exception {
        fill(remoteCache, ASYNC_OPS_ENTRY_LOAD);
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));

        NotifyingFuture<Void> future = remoteCache.clearAsync();
        future.get();

        assertEquals(0, numEntriesOnServer(0));
    }

    @Test
    public void testPutAsync() throws Exception {
        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.putAsync("key" + i, "value" + i));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));
        assertEquals("value" + (ASYNC_OPS_ENTRY_LOAD - 1), remoteCache.get("key" + (ASYNC_OPS_ENTRY_LOAD - 1)));
    }

    @Test
    public void testPutWithLifespanAsync() throws Exception {
        long lifespanInSecs = 10;
        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.putAsync("key" + i, "value" + i, lifespanInSecs, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));
        sleepForSecs(lifespanInSecs + 1);
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            assertEquals(null, remoteCache.get("key" + i));
        }
    }

    @Test
    public void testPutIfAbsentAsync() throws Exception {
        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.putIfAbsentAsync("key" + i, "value" + i));
        }
        // check that the puts completed successfully
        for (Future<?> f : futures) {
            f.get();
        }
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));
        assertEquals("value" + (ASYNC_OPS_ENTRY_LOAD - 1), remoteCache.get("key" + (ASYNC_OPS_ENTRY_LOAD - 1)));
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.putIfAbsentAsync("key" + i, "newValue" + i));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            assertEquals("value" + i, remoteCache.get("key" + i));
        }
    }

    @Test
    public void testPutIfAbsentWithLifespanAsync() throws Exception {
        long lifespanInSecs = 2;
        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.putIfAbsentAsync("key" + i, "value" + i, lifespanInSecs, TimeUnit.SECONDS));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));
        assertEquals("value" + (ASYNC_OPS_ENTRY_LOAD - 1), remoteCache.get("key" + (ASYNC_OPS_ENTRY_LOAD - 1)));
        sleepForSecs(lifespanInSecs + 1);
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            assertEquals(null, remoteCache.get("key" + i));
        }
    }

    @Test
    public void testReplaceWithVersionWithLifespanAsync() throws Exception {
        int lifespanInSecs = 2;
        assertNull(remoteCache.replace("aKey", "aValue"));

        remoteCache.put("aKey", "aValue");
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        NotifyingFuture<Boolean> future = remoteCache.replaceWithVersionAsync("aKey", "aNewValue", valueBinary.getVersion(),
                lifespanInSecs);
        assertTrue(future.get());

        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertEquals("aNewValue", entry2.getValue());

        sleepForSecs(lifespanInSecs + 1);
        assertNull(remoteCache.getVersioned("aKey"));
    }

    @Test
    public void testGetAsync() throws Exception {
        fill(remoteCache, ASYNC_OPS_ENTRY_LOAD);
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));

        Set<Future<?>> futures = new HashSet<Future<?>>();
        for (int i = 0; i != ASYNC_OPS_ENTRY_LOAD; i++) {
            futures.add(remoteCache.getAsync("key" + i));
        }
        for (Future<?> f : futures) {
            assertNotNull(f.get());
        }
    }

    @Test
    public void testBulkOperationsAsync() throws Exception {
        Map<String, String> mapIn = new HashMap<String, String>();
        Map<String, String> mapOut = new HashMap<String, String>();
        fill(mapOut, ASYNC_OPS_ENTRY_LOAD);
        NotifyingFuture<Void> future = remoteCache.putAllAsync(mapOut);
        future.get();

        mapIn = remoteCache.getBulk();
        assertEquals(mapOut, mapIn);
    }

    @Test
    public void testBulkOperationsWithLifespanAsync() throws Exception {
        long lifespanInSecs = 3;
        Map<String, String> mapIn = new HashMap<String, String>();
        Map<String, String> mapOut = new HashMap<String, String>();
        fill(mapOut, ASYNC_OPS_ENTRY_LOAD);
        NotifyingFuture<Void> future = remoteCache.putAllAsync(mapOut, lifespanInSecs, TimeUnit.SECONDS);
        future.get();

        sleepForSecs(lifespanInSecs + 2);
        mapIn = remoteCache.getBulk();
        assertEquals(0, mapIn.size());
    }

    @Test
    public void testReplaceWithVersionAsync() throws Exception {
        assertNull(remoteCache.replace("aKey", "aValue"));

        remoteCache.put("aKey", "aValue");
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        // replacement should take place (and so return true)
        NotifyingFuture<Boolean> future = remoteCache.replaceWithVersionAsync("aKey", "aNewValue", valueBinary.getVersion());
        assertTrue(future.get());

        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertEquals("aNewValue", entry2.getValue());

        // replacement should not take place because we have changed the value
        future = remoteCache.replaceWithVersionAsync("aKey", "aNewValue", valueBinary.getVersion());
        assertFalse(future.get());
    }

    @Test
    public void testRemoveWithVersionAsync() throws Exception {
        NotifyingFuture<Boolean> future = null;
        future = remoteCache.removeWithVersionAsync("aKey", 12321212l);
        assertTrue(!future.get());

        remoteCache.put("aKey", "aValue");
        VersionedValue valueBinary = remoteCache.getVersioned("aKey");
        future = remoteCache.removeWithVersionAsync("aKey", valueBinary.getVersion());
        assertTrue(future.get());

        remoteCache.put("aKey", "aNewValue");
        VersionedValue entry2 = remoteCache.getVersioned("aKey");
        assertNotEquals(entry2.getVersion(), valueBinary.getVersion());
        assertEquals(entry2.getValue(), "aNewValue");

        future = remoteCache.removeWithVersionAsync("aKey", valueBinary.getVersion());
        assertTrue(!future.get());
    }

    /*
     * Test getVersion method - it returns the infinispan version, which we're comparing with the version we have set
     * in the testsuite pom - so it also works as a simple check for us
     *
     * Leftover from migration:
     * Ignore? Do we still need this check in Infinispan server? Or add/change to any other version check?
     */
    @Ignore
    @Test
    public void testGetVersion() throws Exception {
        assertEquals(System.getProperty("version.infinispan"), remoteCache.getVersion());
    }

    @Test
    public void testGetProtocolVersion() throws Exception {
        assertEquals("HotRod client, protocol version :1.3", remoteCache.getProtocolVersion());
    }

    protected <T extends Map<String, String>> void fill(T map, int entryCount) {
        for (int i = 0; i != entryCount; i++) {
            map.put("key" + i, "value" + i);
        }
    }

    protected void sleepForSecs(long numSecs) {
        // give the elements time to be evicted
        try {
            Thread.sleep(numSecs * 1000);
        } catch (InterruptedException e) {
        }
    }

}
