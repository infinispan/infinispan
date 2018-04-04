package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests functionality related to various bulk operations in different cache modes
 *
 * @author William Burns
 * @since 9.1
 */
@Test(groups = "functional", testName = "org.infinispan.client.hotrod.BulkOperationsTest")
public class BulkOperationsTest extends MultipleCacheManagersTest {

   enum CollectionOp {
      ENTRYSET(RemoteCache::entrySet) {
         @Override
         ProtocolVersion minimumVersionForIteration() {
            return ProtocolVersion.PROTOCOL_VERSION_23;
         }
      },
      KEYSET(RemoteCache::keySet) {
         @Override
         ProtocolVersion minimumVersionForIteration() {
            return ProtocolVersion.PROTOCOL_VERSION_12;
         }
      },
      VALUES(RemoteCache::values) {
         @Override
         ProtocolVersion minimumVersionForIteration() {
            return ProtocolVersion.PROTOCOL_VERSION_23;
         }
      };

      private Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>> function;

      abstract ProtocolVersion minimumVersionForIteration();

      CollectionOp(Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>> function) {
         this.function = function;
      }
   }

   enum ItemTransform {
      IDENTITY(Function.identity()),
      COPY_ENTRY((Function) o -> new AbstractMap.SimpleEntry<>(o, o));

      private final Function<Object, Object> function;

      ItemTransform(Function<Object, Object> function) {
         this.function = function;
      }
   }

   protected HotRodServer[] hotrodServers;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected ControlledTimeService timeService;

   @Override
   public Object[] factory() {
      return new Object[] {
            new BulkOperationsTest().cacheMode(CacheMode.DIST_SYNC),
            new BulkOperationsTest().cacheMode(CacheMode.REPL_SYNC),
            new BulkOperationsTest().cacheMode(CacheMode.LOCAL),
      };
   }

   protected int numberOfHotRodServers() {
      return cacheMode.isClustered() ? 3 : 1;
   }

   protected ConfigurationBuilder clusterConfig() {
      return hotRodCacheConfiguration(cacheMode.isClustered() ? getDefaultClusteredCacheConfig(
            cacheMode, false) :  new ConfigurationBuilder());
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      final int numServers = numberOfHotRodServers();
      hotrodServers = new HotRodServer[numServers];

      createCluster(hotRodCacheConfiguration(clusterConfig()), numberOfHotRodServers());

      timeService = new ControlledTimeService();

      for (int i = 0; i < numServers; i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         hotrodServers[i] = HotRodClientTestingUtil.startHotRodServer(cm);
         TestingUtil.replaceComponent(cm.getCache(), TimeService.class, timeService, true);
      }

      String servers = HotRodClientTestingUtil.getServersString(hotrodServers);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServers(servers);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }



   @AfterMethod
   public void checkNoActiveIterations() {
      for (HotRodServer hotRodServer : hotrodServers) {
         assertEquals(0, hotRodServer.getIterationManager().activeIterations());
      }
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServers);
   }

   protected void populateCacheManager() {
      for (int i = 0; i < 100; i++) {
         remoteCache.put(i, i);
      }
   }

   @DataProvider(name = "collections-item")
   public Object[][] collectionItemProvider() {
      return new Object[][] {
            {CollectionOp.KEYSET, ItemTransform.IDENTITY },
            {CollectionOp.VALUES, ItemTransform.IDENTITY },
            {CollectionOp.ENTRYSET, ItemTransform.COPY_ENTRY},
      };
   }

   @Test(dataProvider = "collections-item")
   public void testContains(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      Collection<?> collection = op.function.apply(remoteCache);
      for (int i = 0; i < 100; i++) {
         assertTrue(collection.contains(transform.function.apply(i)));
      }
      assertFalse(collection.contains(transform.function.apply(104)));
      assertFalse(collection.contains(transform.function.apply(-1)));
   }

   @Test(dataProvider = "collections-item")
   public void testContainsAll(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      Collection<?> collection = op.function.apply(remoteCache);
      assertFalse(collection.containsAll(Arrays.asList(transform.function.apply(204), transform.function.apply(4))));
      assertTrue(collection.containsAll(Arrays.asList(transform.function.apply(4), transform.function.apply(10))));
      assertTrue(collection.containsAll(IntStream.range(0, 100).mapToObj(transform.function::apply).collect(Collectors.toList())));
   }

   @Test(dataProvider = "collections-item")
   public void testRemove(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      Collection<?> collection = op.function.apply(remoteCache);
      collection.remove(transform.function.apply(4));
      collection.remove(transform.function.apply(23));
      collection.remove(transform.function.apply(1001));
      assertEquals(98, collection.size());
   }

   @Test(dataProvider = "collections-item")
   public void testRemoveAll(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      CloseableIteratorCollection<?> collection = op.function.apply(remoteCache);
      // 105 can't be removed
      collection.removeAll(Arrays.asList(transform.function.apply(5), transform.function.apply(10),
                                         transform.function.apply(23), transform.function.apply(18),
                                         transform.function.apply(105)));
      assertEquals(96, collection.size());
      collection.removeAll(Arrays.asList(transform.function.apply(5), transform.function.apply(890)));
      assertEquals(96, collection.size());
   }

   @Test(dataProvider = "collections-item")
   public void testRetainAll(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      Collection<?> collection = op.function.apply(remoteCache);
      collection.retainAll(Arrays.asList(transform.function.apply(1), transform.function.apply(23), transform.function.apply(102)));
      assertEquals(2, collection.size());
   }

   @Test(dataProvider = "collections-item", expectedExceptions = UnsupportedOperationException.class)
   public void testAdd(CollectionOp op, ItemTransform transform) {
      CloseableIteratorCollection collection = op.function.apply(remoteCache);
      collection.add(transform.function.apply(1));
   }

   @Test(dataProvider = "collections-item", expectedExceptions = UnsupportedOperationException.class)
   public void testAddAll(CollectionOp op, ItemTransform transform) {
      CloseableIteratorCollection collection = op.function.apply(remoteCache);
      collection.addAll(Arrays.asList(transform.function.apply(1), transform.function.apply(2)));
   }

   @Test(dataProvider = "collections-item")
   public void testStreamAll(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      Collection<?> collection = op.function.apply(remoteCache);
      // Test operator that is performed on all
      // We don't try resource stream to verify iterator is closed upon full consumption
      List<String> strings = collection.stream().map(transform.function).map(Object::toString).collect(Collectors.toList());
      assertEquals(100, strings.size());

      // Test parallel operator that is performed on all
      // We don't try resource stream to verify iterator is closed upon full consumption
      assertEquals(100, collection.parallelStream().count());
   }

   @Test(dataProvider = "collections-item")
   public void testStreamShortCircuit(CollectionOp op, ItemTransform transform) {
      populateCacheManager();
      CloseableIteratorCollection<?> collection = op.function.apply(remoteCache);

      try (Stream<?> stream = collection.stream()) {
         // Test short circuit (non parallel) - it can't match all
         assertEquals(false, stream.allMatch(o -> Objects.equals(o, transform.function.apply(1))));
      }

      try (Stream<?> stream = collection.parallelStream()) {
         // Test short circuit (parallel) - should go through almost all until it finds 4
         assertEquals(transform.function.apply(4),
                      stream.filter(o -> Objects.equals(o, transform.function.apply(4)))
                            .findAny()
                            .get());
      }
   }

   @DataProvider(name = "collections")
   public Object[][] collectionProvider() {
      return new Object[][] {
            {CollectionOp.ENTRYSET},
            {CollectionOp.KEYSET},
            {CollectionOp.VALUES },
      };
   }

   @Test(dataProvider = "collections")
   public void testSizeWithExpiration(CollectionOp op) {
      Map<String, String> dataIn = new HashMap<>();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");
      final long lifespan = 5000;
      remoteCache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      assertEquals(2, op.function.apply(remoteCache).size());

      timeService.advance(lifespan + 1);

      assertEquals(0, op.function.apply(remoteCache).size());
   }

   @Test(dataProvider = "collections")
   public void testIteratorRemove(CollectionOp op) {
      populateCacheManager();
      CloseableIteratorCollection<?> collection = op.function.apply(remoteCache);
      assertEquals(100, collection.size());
      try (CloseableIterator<?> iter = collection.iterator()) {
         assertTrue(iter.hasNext());
         assertNotNull(iter.next());
         Object removed = iter.next();
         assertNotNull(removed);
         iter.remove();
         assertTrue(iter.hasNext());
      }
      assertEquals(99, collection.size());
   }

   @Test(dataProvider = "collections")
   public void testClear(CollectionOp op) {
      populateCacheManager();
      CloseableIteratorCollection<?> collection = op.function.apply(remoteCache);
      assertEquals(100, collection.size());
      collection.clear();
      assertEquals(0, remoteCache.size());
   }

   @DataProvider(name = "collectionsAndVersion")
   public Object[][] collectionAndVersionsProvider() {
      return Arrays.stream(CollectionOp.values())
            .flatMap(op -> Arrays.stream(ProtocolVersion.values())
                  .map(v -> new Object[] {op, v}))
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "collectionsAndVersion")
   public void testIteration(CollectionOp op, ProtocolVersion version) throws IOException {
      Map<String, String> dataIn = new HashMap<>();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");

      RemoteCache<Object, Object> cacheToUse;
      RemoteCacheManager temporaryManager;

      if (version != ProtocolVersion.DEFAULT_PROTOCOL_VERSION) {
         String servers = HotRodClientTestingUtil.getServersString(hotrodServers);

         org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
               new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
         // Set the version on the manager to connect with
         clientBuilder.version(version);
         clientBuilder.addServers(servers);
         temporaryManager = new RemoteCacheManager(clientBuilder.build());
         cacheToUse = temporaryManager.getCache();
      } else {
         temporaryManager = null;
         cacheToUse = remoteCache;
      }

      try {
         // putAll doesn't work in older versions (so we use new client) - that is a different issue completely
         remoteCache.putAll(dataIn);

         CloseableIteratorCollection<?> collection = op.function.apply(cacheToUse);
         // If we don't support it we should get an exception
         if (version.compareTo(op.minimumVersionForIteration()) < 0) {
            Exceptions.expectException(UnsupportedOperationException.class, () -> {
               try (CloseableIterator<?> iter = collection.iterator()) {
               }
            });
         } else {
            try (CloseableIterator<?> iter = collection.iterator()) {
               assertTrue(iter.hasNext());
               assertNotNull(iter.next());
               assertTrue(iter.hasNext());
            }
         }
      } finally {
         if (temporaryManager != null) {
            temporaryManager.close();
         }
      }
   }
}
