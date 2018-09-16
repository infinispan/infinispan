package org.infinispan.server.test.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for HotRod client and its RemoteCache API. Subclasses must provide a
 * way to get the list of remote HotRod servers and to assert the cache is
 * empty.
 * <p>
 * Subclasses may be used in Client-Server mode or Hybrid mode where HotRod
 * server runs as a library deployed in an application server.
 *
 * @author Richard Achmatowicz
 * @author Martin Gencur
 * @author Jozef Vilkolak
 */
public abstract class AbstractRemoteCacheIT {
    private static final Log log = LogFactory.getLog(AbstractRemoteCacheIT.class);
    protected static String testCache = "default";
    protected static RemoteCacheManager remoteCacheManager = null;
    protected RemoteCache remoteCache;
    protected final int ASYNC_OPS_ENTRY_LOAD = 10;

    protected abstract List<RemoteInfinispanServer> getServers();

    @Before
    public void initialize() {
        if (remoteCacheManager == null) {
            Configuration config = createRemoteCacheManagerConfiguration();
            remoteCacheManager = new RemoteCacheManager(config, true);
        }
        remoteCache = remoteCacheManager.getCache(testCache);
        remoteCache.clear();
    }

    @AfterClass
    public static void release() {
        if (remoteCacheManager != null) {
            remoteCacheManager.stop();
            remoteCacheManager = null;
        }
    }

   protected static Archive<?> createPojoArchive() {
      return ShrinkWrap.create(JavaArchive.class, "pojo.jar")
            .addClasses(Person.class, Id.class);
   }


   protected static Archive<?> createFilterArchive() {
        return ShrinkWrap.create(JavaArchive.class, "filter.jar")
                .addClasses(StaticCacheEventFilterFactory.class, DynamicCacheEventFilterFactory.class,
                        CustomPojoEventFilterFactory.class)
                .addAsServiceProvider(CacheEventFilterFactory.class,
                        StaticCacheEventFilterFactory.class, DynamicCacheEventFilterFactory.class,
                        CustomPojoEventFilterFactory.class)
              .add(new StringAsset("Dependencies: deployment.pojo.jar"), "META-INF/MANIFEST.MF");
    }

    protected static Archive<?> createConverterArchive() {
        return ShrinkWrap.create(JavaArchive.class, "converter.jar")
                .addClasses(StaticCacheEventConverterFactory.class, DynamicCacheEventConverterFactory.class,
                        CustomPojoEventConverterFactory.class, CustomEvent.class)
                .addAsServiceProvider(CacheEventConverterFactory.class,
                        StaticCacheEventConverterFactory.class, DynamicCacheEventConverterFactory.class,
                        CustomPojoEventConverterFactory.class)
              .add(new StringAsset("Dependencies: deployment.pojo.jar"), "META-INF/MANIFEST.MF");

    }

    protected static Archive<?> createFilterConverterArchive() {
        return ShrinkWrap.create(JavaArchive.class, "filter-converter.jar")
                .addClasses(FilterConverterFactory.class, CustomEvent.class,
                        CustomPojoFilterConverterFactory.class)
                .addAsServiceProvider(CacheEventFilterConverterFactory.class, FilterConverterFactory.class,
                        CustomPojoFilterConverterFactory.class)
              .add(new StringAsset("Dependencies: deployment.pojo.jar"), "META-INF/MANIFEST.MF");

    }

    protected static Archive<?> createKeyValueFilterConverterArchive() {
        return ShrinkWrap.create(JavaArchive.class, "key-value-filter-converter.jar")
                .addClasses(TestKeyValueFilterConverterFactory.class, SampleEntity.class, Summary.class, SampleEntity.SampleEntityExternalizer.class, Summary.SummaryExternalizer.class)
                .addAsServiceProvider(KeyValueFilterConverterFactory.class, TestKeyValueFilterConverterFactory.class)
              .add(new StringAsset("Dependencies: deployment.pojo.jar"), "META-INF/MANIFEST.MF");

    }

    private Configuration createRemoteCacheManagerConfiguration(int... hotrodPortOverrides) {
        if (hotrodPortOverrides.length != 0) {
            assert getServers().size() == hotrodPortOverrides.length : "The number of defined ports is different from server count";
        }
        ConfigurationBuilder config = new ConfigurationBuilder();
        int index = 0;
        for (RemoteInfinispanServer server : getServers()) {
            int port = hotrodPortOverrides.length != 0 ? hotrodPortOverrides[index] : server.getHotrodEndpoint().getPort();
            config.addServer()
                    .host(server.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(port);
            ++index;
        }
        config.balancingStrategy("org.infinispan.server.test.client.hotrod.HotRodTestRequestBalancingStrategy")
                // load balancing
                .balancingStrategy("org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy")
                .forceReturnValues(false)
                        // TCP stuff
                .tcpNoDelay(true)
                .transportFactory("org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory")
                        // marshalling
                        // FIXME Workaround for ISPN-6367
                .marshaller(new GenericJBossMarshaller(Thread.currentThread().getContextClassLoader()))
                        // executors
                .asyncExecutorFactory().factoryClass("org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.pool_size", "10")
                .addExecutorProperty("infinispan.client.hotrod.default_executor_factory.queue_size", "100000")
                        //hashing
                .keySizeEstimate(64)
                .valueSizeEstimate(512);
        return config.build();
    }

    private long numEntriesOnServer(int serverIndex) {
        return getServers().get(serverIndex).
              getCacheManager(getCacheManagerName()).
              getCache(testCache).getNumberOfEntries();
    }

    protected String getCacheManagerName() {
        return "clustered";
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
    public void testReplaceOldValue() throws Exception {
        remoteCache.put("aKey", "aValue");
        VersionedValue previous = remoteCache.getWithMetadata("aKey");
        assertTrue(remoteCache.replace("aKey", "aValue", "aNewValue"));


        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getWithMetadata("aKey");
        assertNotEquals(entry2.getVersion(), previous.getVersion());
        assertEquals("aNewValue", entry2.getValue());
    }

    @Test
    public void testReplaceOldValueWithLifespan() throws Exception {
        int lifespanInMillis = 10;
        remoteCache.put("aKey", "aValue");
        VersionedValue previous = remoteCache.getWithMetadata("aKey");
        assertTrue(remoteCache.replace("aKey", "aValue", "aNewValue", lifespanInMillis, TimeUnit.MILLISECONDS));


        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getWithMetadata("aKey");
        assertNotEquals(entry2.getVersion(), previous.getVersion());
        assertEquals("aNewValue", entry2.getValue());

        Thread.sleep(2 * lifespanInMillis);
        assertNull(remoteCache.getWithMetadata("aKey"));
    }

    @Test
    public void testReplaceOldValueWithLifespanAndExpiration() throws Exception {
        int lifespanInMillis = 10;
        remoteCache.put("aKey", "aValue");
        VersionedValue previous = remoteCache.getWithMetadata("aKey");
        // Max idle should expire
        assertTrue(remoteCache.replace("aKey", "aValue", "aNewValue", lifespanInMillis, TimeUnit.MINUTES,
              lifespanInMillis, TimeUnit.MILLISECONDS));


        // version should have changed; value should have changed
        VersionedValue entry2 = remoteCache.getWithMetadata("aKey");
        assertNotEquals(entry2.getVersion(), previous.getVersion());
        assertEquals("aNewValue", entry2.getValue());

        Thread.sleep(2 * lifespanInMillis);
        assertNull(remoteCache.getWithMetadata("aKey"));
    }

    @Test
    public void testPut() throws Exception {
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
    public void testContainsValue() {
        assertTrue(!remoteCache.containsValue("aValue"));
        remoteCache.put("aKey", "aValue");
        assertTrue(remoteCache.containsValue("aValue"));
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
        assertEquals(testCache, remoteCache.getName());
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
    public void testEntrySet() {
        remoteCache.put("k1", "v1");
        remoteCache.put("k2", "v2");
        remoteCache.put("k3", "v3");

        Set<Map.Entry<String, String>> expectedEntrySet = new HashSet<>();
        expectedEntrySet.add(new AbstractMap.SimpleEntry<>("k1", "v1"));
        expectedEntrySet.add(new AbstractMap.SimpleEntry<>("k2", "v2"));
        expectedEntrySet.add(new AbstractMap.SimpleEntry<>("k3", "v3"));
        assertEquals(expectedEntrySet, remoteCache.entrySet());
    }

    @Test
    public void testValues() {
        remoteCache.put("k1", "v1");
        remoteCache.put("k2", "v2");
        remoteCache.put("k3", "v3");

        List<String> values = ((RemoteCache<?, String>) remoteCache).values().stream().collect(Collectors.toList());
        assertEquals(3, values.size());
        assertTrue(values.contains("v1"));
        assertTrue(values.contains("v2"));
        assertTrue(values.contains("v3"));
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
    public void testRemoveConditional() throws IOException {
        assertNull(remoteCache.put("aKey", "aValue"));
        assertEquals(remoteCache.get("aKey"), "aValue");

        assertFalse(remoteCache.remove("aKey", "aValue2"));
        assertTrue(remoteCache.containsKey("aKey"));
        assertTrue(remoteCache.remove("aKey", "aValue"));
        assertFalse(remoteCache.containsKey("aKey"));
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
        log.tracef("named stats = %s", remoteStats.getStatsMap());
    }

    @Test
    public void testUnsupportedOperations() {

        try {
            remoteCache.removeAsync("aKey", "aValue");
            fail("call to removeAsync() did not raise UnsupportedOperationException ");
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
    }

    @Test
    public void testClearAsync() throws Exception {
        fill(remoteCache, ASYNC_OPS_ENTRY_LOAD);
        assertEquals(ASYNC_OPS_ENTRY_LOAD, numEntriesOnServer(0));

        CompletableFuture<Void> future = remoteCache.clearAsync();
        future.get();

        assertEquals(0, numEntriesOnServer(0));
    }

    @Test
    public void testPutAsync() throws Exception {
        assertNull(remoteCache.get("k"));
        Future<String> f = remoteCache.putAsync("k", "v");
        assertFalse(f.isCancelled());
        assertNull(f.get());
        assertTrue(f.isDone());
        assertEquals("v", remoteCache.get("k"));
    }

    @Test
    public void testPutWithLifespanAsync() throws Exception {
        long lifespanInSecs = 2;
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
        CompletableFuture<Boolean> future = remoteCache.replaceWithVersionAsync("aKey", "aNewValue", valueBinary.getVersion(),
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
        CompletableFuture<Void> future = remoteCache.putAllAsync(mapOut);
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
        CompletableFuture<Void> future = remoteCache.putAllAsync(mapOut, lifespanInSecs, TimeUnit.SECONDS);
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
        CompletableFuture<Boolean> future = remoteCache.replaceWithVersionAsync("aKey", "aNewValue", valueBinary.getVersion());
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
        CompletableFuture<Boolean> future = null;
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

    @Test
    public void testGetProtocolVersion() throws Exception {
       assertEquals("HotRod client, protocol version: " + ProtocolVersion.DEFAULT_PROTOCOL_VERSION, remoteCache.getProtocolVersion());
    }

    @Test
    public void testPutGetCustomObject() throws Exception {
        final Person p = new Person("Martin");
        remoteCache.put("k1", p);
        assertEquals(p, remoteCache.get("k1"));
    }

    @Test
    public void testEventReceiveBasic() {
        final EventLogListener eventListener = new EventLogListener();
        remoteCache.addClientListener(eventListener);
        try {
            expectNoEvents(eventListener);
            // Created events
            remoteCache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener);
            remoteCache.put(2, "two");
            expectOnlyCreatedEvent(2, eventListener);
            // Modified events
            remoteCache.put(1, "newone");
            expectOnlyModifiedEvent(1, eventListener);
            // Remove events
            remoteCache.remove(1);
            expectOnlyRemovedEvent(1, eventListener);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testEventReceiveConditional() {
        final EventLogListener eventListener = new EventLogListener();
        remoteCache.addClientListener(eventListener);
        try {
            expectNoEvents(eventListener);
            // Put if absent
            remoteCache.putIfAbsent(1, "one");
            expectOnlyCreatedEvent(1, eventListener);
            remoteCache.putIfAbsent(1, "again");
            expectNoEvents(eventListener);
            // Replace
            remoteCache.replace(1, "newone");
            expectOnlyModifiedEvent(1, eventListener);
            // Replace with version
            remoteCache.replaceWithVersion(1, "one", 0);
            expectNoEvents(eventListener);
            VersionedValue<String> versioned = remoteCache.getVersioned(1);
            remoteCache.replaceWithVersion(1, "one", versioned.getVersion());
            expectOnlyModifiedEvent(1, eventListener);
            // Remove with version
            remoteCache.removeWithVersion(1, 0);
            expectNoEvents(eventListener);
            versioned = remoteCache.getVersioned(1);
            remoteCache.removeWithVersion(1, versioned.getVersion());
            expectOnlyRemovedEvent(1, eventListener);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testEventFilteringStatic() {
        final StaticFilteredEventLogListener eventListener = new StaticFilteredEventLogListener();
        remoteCache.addClientListener(eventListener);
        try {
            expectNoEvents(eventListener);
            remoteCache.put(1, "one");
            expectNoEvents(eventListener);
            remoteCache.put(2, "two");
            expectOnlyCreatedEvent(2, eventListener);
            remoteCache.remove(1);
            expectNoEvents(eventListener);
            remoteCache.remove(2);
            expectOnlyRemovedEvent(2, eventListener);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testEventFilteringDynamic() {
        final DynamicFilteredEventLogListener eventListener = new DynamicFilteredEventLogListener();
        remoteCache.addClientListener(eventListener, new Object[]{3}, null);
        try {
            expectNoEvents(eventListener);
            remoteCache.put(1, "one");
            expectNoEvents(eventListener);
            remoteCache.put(2, "two");
            expectNoEvents(eventListener);
            remoteCache.put(3, "three");
            expectOnlyCreatedEvent(3, eventListener);
            remoteCache.replace(1, "new-one");
            expectNoEvents(eventListener);
            remoteCache.replace(2, "new-two");
            expectNoEvents(eventListener);
            remoteCache.replace(3, "new-three");
            expectOnlyModifiedEvent(3, eventListener);
            remoteCache.remove(1);
            expectNoEvents(eventListener);
            remoteCache.remove(2);
            expectNoEvents(eventListener);
            remoteCache.remove(3);
            expectOnlyRemovedEvent(3, eventListener);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testCustomEvents() {
        final StaticCustomEventLogListener eventListener = new StaticCustomEventLogListener();
        remoteCache.addClientListener(eventListener);
        try {
            eventListener.expectNoEvents();
            remoteCache.put(1, "one");
            eventListener.expectSingleCustomEvent(1, "one");
            remoteCache.put(1, "newone");
            eventListener.expectSingleCustomEvent(1, "newone");
            remoteCache.remove(1);
            eventListener.expectSingleCustomEvent(1, null);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testCustomEventsDynamic() {
        final DynamicCustomEventLogListener eventListener = new DynamicCustomEventLogListener();
        remoteCache.addClientListener(eventListener, null, new Object[]{2});
        try {
            eventListener.expectNoEvents();
            remoteCache.put(1, "one");
            eventListener.expectSingleCustomEvent(1, "one");
            remoteCache.put(2, "two");
            eventListener.expectSingleCustomEvent(2, null);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testCustomFilterEvents() {
        final FilterCustomEventLogListener eventListener = new FilterCustomEventLogListener();
        remoteCache.addClientListener(eventListener, new Object[]{3}, null);
        try {
            eventListener.expectNoEvents();
            remoteCache.put(1, "one");
            eventListener.expectSingleCustomEvent(1, "one");
            remoteCache.put(1, "uno");
            eventListener.expectSingleCustomEvent(1, "uno");
            remoteCache.put(2, "two");
            eventListener.expectSingleCustomEvent(2, "two");
            remoteCache.put(2, "dos");
            eventListener.expectSingleCustomEvent(2, "dos");
            remoteCache.put(3, "three");
            eventListener.expectSingleCustomEvent(3, null);
            remoteCache.put(3, "tres");
            eventListener.expectSingleCustomEvent(3, null);
            remoteCache.remove(1);
            eventListener.expectSingleCustomEvent(1, null);
            remoteCache.remove(2);
            eventListener.expectSingleCustomEvent(2, null);
            remoteCache.remove(3);
            eventListener.expectSingleCustomEvent(3, null);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

   @Test
    public void testIterationWithCustomClasses() {
       remoteCache.put("1", new SampleEntity("value1,value2"));
       remoteCache.put("2", new SampleEntity("value3,value2"));
       remoteCache.put("ignore", new SampleEntity("whatever"));
       remoteCache.put("3", new SampleEntity("value7,value8"));

       final Map<Object, Object> entryMap = new HashMap<>();
       try(CloseableIterator<Entry<Object, Object>> closeableIterator = remoteCache.retrieveEntries("csv-key-value-filter-converter-factory", null, 10)) {
          closeableIterator.forEachRemaining(new Consumer<Entry<Object, Object>>() {
             @Override
             public void accept(Entry<Object, Object> e) {
                entryMap.put(e.getKey(), e.getValue());
             }
          });
       }

      assertEquals(3, entryMap.size());
      assertEquals(Arrays.asList("value1","value2"), ((Summary) entryMap.get("1")).getAttributes());
      assertEquals(Arrays.asList("value3","value2"), ((Summary) entryMap.get("2")).getAttributes());
      assertEquals(Arrays.asList("value7","value8"), ((Summary) entryMap.get("3")).getAttributes());
    }

    @Test
    public void testEventFilteringCustomPojo() {
        final CustomPojoFilteredEventLogListener eventListener = new CustomPojoFilteredEventLogListener();
        remoteCache.addClientListener(eventListener, new Object[]{"two"}, null);
        try {
            expectNoEvents(eventListener);
            remoteCache.put(1, new Person("one"));
            expectNoEvents(eventListener);
            remoteCache.put(2, new Person("two"));
            expectOnlyCreatedEvent(2, eventListener);
            remoteCache.remove(1);
            expectNoEvents(eventListener);
            remoteCache.remove(2);
            expectOnlyRemovedEvent(2, eventListener);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testCustomEventsCustomPojo() {
        final CustomPojoCustomEventLogListener eventListener = new CustomPojoCustomEventLogListener();
        remoteCache.addClientListener(eventListener, null, new Object[]{new Person("two")});
        try {
            eventListener.expectNoEvents();
            remoteCache.put(1, new Person("one"));
            eventListener.expectSingleCustomEvent(1, new Person("one"));
            remoteCache.put(2, new Person("two"));
            eventListener.expectSingleCustomEvent(2, null);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    public void testCustomFilterEventsCustomPojo() {
        final CustomPojoFilterCustomEventLogListener eventListener = new CustomPojoFilterCustomEventLogListener();
        remoteCache.addClientListener(eventListener, new Object[]{new Id(3)}, null);
        try {
            eventListener.expectNoEvents();
            remoteCache.put(new Id(1), new Person("one"));
            eventListener.expectSingleCustomEvent(new Id(1), new Person("one"));
            remoteCache.put(new Id(1), new Person("uno"));
            eventListener.expectSingleCustomEvent(new Id(1), new Person("uno"));
            remoteCache.put(new Id(2), new Person("two"));
            eventListener.expectSingleCustomEvent(new Id(2), new Person("two"));
            remoteCache.put(new Id(2), new Person("dos"));
            eventListener.expectSingleCustomEvent(new Id(2), new Person("dos"));
            remoteCache.put(new Id(3), new Person("three"));
            eventListener.expectSingleCustomEvent(new Id(3), null);
            remoteCache.put(new Id(3), new Person("tres"));
            eventListener.expectSingleCustomEvent(new Id(3), null);
            remoteCache.remove(new Id(1));
            eventListener.expectSingleCustomEvent(new Id(1), null);
            remoteCache.remove(new Id(2));
            eventListener.expectSingleCustomEvent(new Id(2), null);
            remoteCache.remove(new Id(3));
            eventListener.expectSingleCustomEvent(new Id(3), null);
        } finally {
            remoteCache.removeClientListener(eventListener);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDataConversionsWithDefaultRemoteCache() throws Exception {
        String key = "key-byte-array-1";
        byte[] value = {0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x21};  // Hello!
        String stringValue = new String(value);

        DataFormat writeValueUnmarshalled = DataFormat.builder().valueType(APPLICATION_OCTET_STREAM).build();

        // Avoid marshalling values when writing
        RemoteCache<String, byte[]> octetStreamCacheValue = this.remoteCache.withDataFormat(writeValueUnmarshalled);
        octetStreamCacheValue.put(key, value);

        assertArrayEquals(value, octetStreamCacheValue.get(key));

        // Read as UTF
        Object utfValue = this.remoteCache
              .withDataFormat(DataFormat.builder().valueType(TEXT_PLAIN).valueMarshaller(new UTF8StringMarshaller()).build())
              .get(key);

        assertEquals(stringValue, utfValue);

        // Read as XML
        Object xmlValue = this.remoteCache
              .withDataFormat(DataFormat.builder().valueType(APPLICATION_XML).valueMarshaller(new UTF8StringMarshaller()).build())
              .get(key);

        assertEquals("<string>" + stringValue + "</string>", xmlValue);

        // Read as JSON
        Object jsonValue = this.remoteCache
              .withDataFormat(DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build())
              .get(key);

        assertEquals("\"Hello!\"", jsonValue);
    }

    public static <K> void expectOnlyCreatedEvent(K key, EventLogListener eventListener) {
        expectSingleEvent(key, eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
    }

    public static <K> void expectOnlyModifiedEvent(K key, EventLogListener eventListener) {
        expectSingleEvent(key, eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
    }

    public static <K> void expectOnlyRemovedEvent(K key, EventLogListener eventListener) {
        expectSingleEvent(key, eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
    }

    public static <K> void expectSingleEvent(K key, EventLogListener eventListener, ClientEvent.Type type) {
        switch (type) {
            case CLIENT_CACHE_ENTRY_CREATED:
                ClientCacheEntryCreatedEvent createdEvent = eventListener.pollEvent(type);
                assertEquals(key, createdEvent.getKey());
                break;
            case CLIENT_CACHE_ENTRY_MODIFIED:
                ClientCacheEntryModifiedEvent modifiedEvent = eventListener.pollEvent(type);
                assertEquals(key, modifiedEvent.getKey());
                break;
            case CLIENT_CACHE_ENTRY_REMOVED:
                ClientCacheEntryRemovedEvent removedEvent = eventListener.pollEvent(type);
                assertEquals(key, removedEvent.getKey());
                break;
        }
        assertEquals(0, eventListener.queue(type).size());
    }

    public static void expectNoEvents(EventLogListener eventListener) {
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
        expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
    }

    public static void expectNoEvents(EventLogListener eventListener, ClientEvent.Type type) {
        switch (type) {
            case CLIENT_CACHE_ENTRY_CREATED:
                assertEquals(0, eventListener.createdEvents.size());
                break;
            case CLIENT_CACHE_ENTRY_MODIFIED:
                assertEquals(0, eventListener.modifiedEvents.size());
                break;
            case CLIENT_CACHE_ENTRY_REMOVED:
                assertEquals(0, eventListener.removedEvents.size());
                break;
        }
    }

    @ClientListener(filterFactoryName = "static-filter-factory")
    public static class StaticFilteredEventLogListener extends EventLogListener {}

    @ClientListener(filterFactoryName = "dynamic-filter-factory")
    public static class DynamicFilteredEventLogListener extends EventLogListener {}

    @ClientListener(converterFactoryName = "static-converter-factory")
    public static class StaticCustomEventLogListener extends CustomEventLogListener {}

    @ClientListener(converterFactoryName = "dynamic-converter-factory")
    public static class DynamicCustomEventLogListener extends CustomEventLogListener {}

    @ClientListener(filterFactoryName = "filter-converter-factory", converterFactoryName = "filter-converter-factory")
    public static class FilterCustomEventLogListener extends CustomEventLogListener {}

    @ClientListener(filterFactoryName = "pojo-filter-factory")
    public static class CustomPojoFilteredEventLogListener extends EventLogListener {}

    @ClientListener(converterFactoryName = "pojo-converter-factory")
    public static class CustomPojoCustomEventLogListener extends CustomEventLogListener {}

    @ClientListener(filterFactoryName = "pojo-filter-converter-factory", converterFactoryName = "pojo-filter-converter-factory")
    public static class CustomPojoFilterCustomEventLogListener extends CustomEventLogListener {}

    public static class Person implements Serializable {

        final String name;

        public Person(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (!name.equals(person.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    public static class Id implements Serializable {
        final byte id;
        public Id(int id) {
            this.id = (byte) id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Id id1 = (Id) o;

            if (id != id1.id) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    protected <T extends Map<String, String>> void fill(T map, int entryCount) {
        for (int i = 0; i != entryCount; i++) {
            map.put("key" + i, "value" + i);
        }
    }
}
