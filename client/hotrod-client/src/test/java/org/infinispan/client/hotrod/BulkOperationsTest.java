package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

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
            { (Function<RemoteCache<?, ?>, Collection<?>>) RemoteCache::keySet, Function.identity() },
            { (Function<RemoteCache<?, ?>, Collection<?>>) RemoteCache::values, Function.identity() },
            { (Function<RemoteCache<?, ?>, Collection<?>>) RemoteCache::entrySet, (Function) o -> new AbstractMap.SimpleEntry<>(o, o) },
      };
   }

   @Test(dataProvider = "collections-item")
   public void testContains(Function<RemoteCache<?, ?>, Collection<?>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<?> collection = collFunction.apply(remoteCache);
      for (int i = 0; i < 100; i++) {
         assertTrue(collection.contains(itemFunction.apply(i)));
      }
      assertFalse(collection.contains(itemFunction.apply(104)));
      assertFalse(collection.contains(itemFunction.apply(-1)));
   }

   @Test(dataProvider = "collections-item")
   public void testContainsAll(Function<RemoteCache<?, ?>, Collection<?>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<?> collection = collFunction.apply(remoteCache);
      assertFalse(collection.containsAll(Arrays.asList(itemFunction.apply(204), itemFunction.apply(4))));
      assertTrue(collection.containsAll(Arrays.asList(itemFunction.apply(4), itemFunction.apply(10))));
      assertTrue(collection.containsAll(IntStream.range(0, 100).mapToObj(itemFunction::apply).collect(Collectors.toList())));
   }

   @Test(dataProvider = "collections-item")
   public void testRemove(Function<RemoteCache<?, ?>, Collection<?>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<?> collection = collFunction.apply(remoteCache);
      collection.remove(itemFunction.apply(4));
      collection.remove(itemFunction.apply(23));
      collection.remove(itemFunction.apply(1001));
      assertEquals(98, collection.size());
   }

   @Test(dataProvider = "collections-item")
   public void testRemoveAll(Function<RemoteCache<?, ?>, Collection<Object>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<Object> collection = collFunction.apply(remoteCache);
      // 105 can't be removed
      collection.removeAll(Arrays.asList(itemFunction.apply(5), itemFunction.apply(10), itemFunction.apply(23), itemFunction.apply(18), itemFunction.apply(105)));
      assertEquals(96, collection.size());
      collection.removeAll(Arrays.asList(itemFunction.apply(5), itemFunction.apply(890)));
      assertEquals(96, collection.size());
   }

   @Test(dataProvider = "collections-item")
   public void testRetainAll(Function<RemoteCache<?, ?>, Collection<?>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<?> collection = collFunction.apply(remoteCache);
      collection.retainAll(Arrays.asList(itemFunction.apply(1), itemFunction.apply(23), itemFunction.apply(102)));
      assertEquals(2, collection.size());
   }

   @Test(dataProvider = "collections-item", expectedExceptions = UnsupportedOperationException.class)
   public void testAdd(Function<RemoteCache<?, ?>, CloseableIteratorCollection<Object>> function, Function<Object, Object> itemFunction) {
      CloseableIteratorCollection<Object> collection = function.apply(remoteCache);
      collection.add(itemFunction.apply(1));
   }

   @Test(dataProvider = "collections-item", expectedExceptions = UnsupportedOperationException.class)
   public void testAddAll(Function<RemoteCache<?, ?>, CloseableIteratorCollection<Object>> function, Function<Object, Object> itemFunction) {
      CloseableIteratorCollection<Object> collection = function.apply(remoteCache);
      collection.addAll(Arrays.asList(itemFunction.apply(1), itemFunction.apply(2)));
   }

   @Test(dataProvider = "collections-item")
   public void testStreamAll(Function<RemoteCache<?, ?>, Collection<Object>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<Object> collection = collFunction.apply(remoteCache);
      // Test operator that is performed on all
      // We don't try resource stream to verify iterator is closed upon full consumption
      List<String> strings = collection.stream().map(Object::toString).collect(Collectors.toList());
      assertEquals(100, strings.size());

      // Test parallel operator that is performed on all
      // We don't try resource stream to verify iterator is closed upon full consumption
      assertEquals(100, collection.parallelStream().count());
   }

   @Test(dataProvider = "collections-item")
   public void testStreamShortCircuit(Function<RemoteCache<?, ?>, Collection<Object>> collFunction, Function<Object, Object> itemFunction) {
      populateCacheManager();
      Collection<Object> collection = collFunction.apply(remoteCache);

      try (Stream<Object> stream = collection.stream()) {
         // Test short circuit (non parallel) - it can't match all
         assertEquals(false, stream.allMatch(o -> Objects.equals(o, itemFunction.apply(1))));
      }

      try (Stream<Object> stream = collection.parallelStream()) {
         // Test short circuit (parallel) - should go through almost all until it finds 4
         assertEquals(itemFunction.apply(4), stream.filter(o -> Objects.equals(o, itemFunction.apply(4))).findAny().get());
      }
   }

   @DataProvider(name = "collections")
   public Object[][] collectionProvider() {
      return new Object[][] {
            { (Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>>) RemoteCache::entrySet },
            { (Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>>) RemoteCache::keySet },
            { (Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>>) RemoteCache::values },
      };
   }

   @Test(dataProvider = "collections")
   public void testSizeWithExpiration(Function<RemoteCache<?, ?>, Collection<?>> function) {
      Map<String, String> dataIn = new HashMap<>();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");
      final long lifespan = 5000;
      remoteCache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      assertEquals(2, function.apply(remoteCache).size());

      timeService.advance(lifespan + 1);

      assertEquals(0, function.apply(remoteCache).size());
   }

   @Test(dataProvider = "collections")
   public void testIteratorRemove(Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>> function) {
      populateCacheManager();
      CloseableIteratorCollection<?> collection = function.apply(remoteCache);
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
   public void testClear(Function<RemoteCache<?, ?>, CloseableIteratorCollection<?>> function) {
      populateCacheManager();
      CloseableIteratorCollection<?> collection = function.apply(remoteCache);
      assertEquals(100, collection.size());
      collection.clear();
      assertEquals(0, remoteCache.size());
   }
}
