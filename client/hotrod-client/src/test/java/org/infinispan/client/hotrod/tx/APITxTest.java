package org.infinispan.client.hotrod.tx;

import static org.infinispan.client.hotrod.configuration.TransactionMode.FULL_XA;
import static org.infinispan.client.hotrod.configuration.TransactionMode.NON_DURABLE_XA;
import static org.infinispan.client.hotrod.configuration.TransactionMode.NON_XA;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.assertNoTransaction;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.BYTE_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.GENERIC_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.STRING_GENERATOR;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.tx.util.KeyValueGenerator;
import org.infinispan.client.hotrod.tx.util.TransactionSetup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests all the methods, encoding/decoding and transaction isolation.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.tx.APITxTest")
public class APITxTest<K, V> extends MultiHotRodServersTest {

   private static final int NR_NODES = 2;
   private static final String CACHE_NAME = "api-tx-cache";

   private KeyValueGenerator<K, V> kvGenerator;
   private TransactionMode transactionMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            new APITxTest<String, String>().keyValueGenerator(STRING_GENERATOR).transactionMode(NON_XA),
            new APITxTest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).transactionMode(NON_XA),
            new APITxTest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).transactionMode(NON_XA),

            new APITxTest<String, String>().keyValueGenerator(STRING_GENERATOR).transactionMode(NON_DURABLE_XA),
            new APITxTest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).transactionMode(NON_DURABLE_XA),
            new APITxTest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).transactionMode(NON_DURABLE_XA),

            new APITxTest<String, String>().keyValueGenerator(STRING_GENERATOR).transactionMode(FULL_XA),
            new APITxTest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).transactionMode(FULL_XA),
            new APITxTest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).transactionMode(FULL_XA)
      };
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      assertNoTransaction(clients);
      super.clearContent();
   }

   public void testPut(Method method) throws Exception {
      doApiTest(method, this::empty, this::put, this::putDataCheck, this::checkNoKeys, 3, 3, true);
   }

   public void testPutAsync(Method method) throws Exception {
      doApiTest(method, this::empty, this::put, this::putDataCheck, this::checkNoKeys, 3, 3, false);
   }

   public void testPutIfAbsent(Method method) throws Exception {
      doApiTest(method, this::empty, this::putIfAbsent, this::putDataCheck, this::checkNoKeys, 3, 3, true);
   }

   public void testPutIfAbsentAsync(Method method) throws Exception {
      doApiTest(method, this::empty, this::putIfAbsent, this::putDataCheck, this::checkNoKeys, 3, 3, false);
   }

   public void testPutAll(Method method) throws Exception {
      doApiTest(method, this::empty, this::putAll, this::putAllDataCheck, this::checkNoKeys, 6, 6, true);
   }

   public void testPutAllAsync(Method method) throws Exception {
      doApiTest(method, this::empty, this::putAll, this::putAllDataCheck, this::checkNoKeys, 6, 6, false);
   }

   public void testReplace(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::replace, this::secondHalfDataCheck, this::checkInitValue, 6, 12, true);
   }

   public void testReplaceAsync(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::replace, this::secondHalfDataCheck, this::checkInitValue, 6, 12, false);
   }

   public void testReplaceWithVersion(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::replaceWithVersion, this::secondHalfDataCheck, this::checkInitValue, 4, 8,
            true);
   }

   public void testReplaceWithVersionAsync(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::replaceWithVersion, this::secondHalfDataCheck, this::checkInitValue, 4, 8,
            false);
   }

   public void testRemove(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::remove, this::checkNoKeys, this::checkInitValue, 2, 2, true);
   }

   public void testRemoveAsync(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::remove, this::checkNoKeys, this::checkInitValue, 2, 2, false);
   }

   public void testRemoveWithVersion(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::removeWithVersion, this::checkNoKeys, this::checkInitValue, 1, 1, true);
   }

   public void testRemoveWithVersionAsync(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::removeWithVersion, this::checkNoKeys, this::checkInitValue, 1, 1, false);
   }

   public void testMerge(Method method) throws Exception {
      RemoteCache<K, V> cache = txRemoteCache();
      TransactionManager tm = cache.getTransactionManager();

      List<K> keys = generateKeys(method, 3);
      List<V> values = generateValues(method, 3);

      tm.begin();
      //merge is throwing UOE for "normal" remote cache. it isn't supported in tx as well
      expectException(UnsupportedOperationException.class,
            () -> cache.merge(keys.get(0), values.get(0), (v1, v2) -> values.get(0)));
      expectException(UnsupportedOperationException.class,
            () -> cache.merge(keys.get(0), values.get(0), (v1, v2) -> values.get(0), 1, TimeUnit.MINUTES));
      expectException(UnsupportedOperationException.class, () -> cache
            .merge(keys.get(0), values.get(0), (v1, v2) -> values.get(0), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES));
      tm.commit();
   }

   public void testCompute(Method method) throws Exception {
      doApiTest(method, this::empty, this::compute, this::checkInitValue, this::checkNoKeys, 1, 1, true);
   }

   public void testComputeIfAbsent(Method method) throws Exception {
      doApiTest(method, this::empty, this::computeIfAbsent, this::checkInitValue, this::checkNoKeys, 1, 1, true);
   }

   public void testComputeIfPresent(Method method) throws Exception {
      doApiTest(method, this::initKeys, this::computeIfPresent,
            (keys, values, inTx) -> checkInitValue(keys, values.subList(1, 2), inTx), this::checkInitValue, 1, 2,
            true);
   }

   public void testContainsKeyAndValue(Method method) throws Exception {
      RemoteCache<K, V> cache = txRemoteCache();
      TransactionManager tm = cache.getTransactionManager();

      final K key = kvGenerator.generateKey(method, 0);
      final K key1 = kvGenerator.generateKey(method, 1);
      final V value = kvGenerator.generateValue(method, 0);
      final V value1 = kvGenerator.generateValue(method, 1);

      cache.put(key, value);
      cache.put(key1, value1);

      tm.begin();
      assertTrue(cache.containsKey(key));
      assertTrue(cache.containsValue(value));

      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));

      cache.remove(key);

      assertFalse(cache.containsKey(key));
      assertFalse(cache.containsValue(value));
      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));

      final Transaction tx = tm.suspend();

      //check it didn't leak
      assertTrue(cache.containsKey(key));
      assertTrue(cache.containsValue(value));
      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));

      tm.resume(tx);
      tm.commit();

      tm.begin();
      assertFalse(cache.containsKey(key));
      assertFalse(cache.containsValue(value));

      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));

      cache.put(key, value);

      assertTrue(cache.containsKey(key));
      assertTrue(cache.containsValue(value));
      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));

      final Transaction tx2 = tm.suspend();

      //check it didn't leak
      assertFalse(cache.containsKey(key));
      assertFalse(cache.containsValue(value));
      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));

      tm.resume(tx2);
      tm.commit();

      assertTrue(cache.containsKey(key));
      assertTrue(cache.containsValue(value));
      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsValue(value1));
   }

   @Override
   protected boolean cleanupAfterMethod() {
      try {
         cleanupTransactions();
      } catch (SystemException e) {
         log.error("Error cleaning up running transactions", e);
      }
      return super.cleanupAfterMethod();
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), null, null);
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), kvGenerator.toString(), transactionMode);
   }

   @Override
   protected String parameters() {
      return "[" + kvGenerator + "/" + transactionMode + "]";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cacheBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      createHotRodServers(NR_NODES, new ConfigurationBuilder());
      defineInAll(CACHE_NAME, cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
         int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
            .createHotRodClientConfigurationBuilder(serverPort);
      clientBuilder.forceReturnValues(false);
      TransactionSetup.amendJTA(clientBuilder);
      clientBuilder.transaction().transactionMode(transactionMode);
      return clientBuilder;
   }

   private void cleanupTransactions() throws SystemException {
      RemoteCache<K, V> cache = txRemoteCache();
      TransactionManager tm = cache.getTransactionManager();
      if (tm.getTransaction() != null) {
         tm.rollback();
      }
   }

   private APITxTest<K, V> transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   private APITxTest<K, V> keyValueGenerator(KeyValueGenerator<K, V> kvGenerator) {
      this.kvGenerator = kvGenerator;
      return this;
   }

   private void doApiTest(Method method, Step<K, V> init, OperationStep<K, V> op, DataCheck<K, V> dataCheck,
         Step<K, V> isolationCheck, int keysCount, int valuesCount, boolean sync)
         throws Exception {
      RemoteCache<K, V> cache = txRemoteCache();
      TransactionManager tm = cache.getTransactionManager();

      List<K> keys = generateKeys(method, keysCount);
      List<V> values = generateValues(method, valuesCount);

      init.execute(keys, values);

      tm.begin();
      op.execute(keys, values, sync);
      dataCheck.execute(keys, values, true);

      final Transaction tx = tm.suspend();

      //check it didn't leak outside the transaction
      isolationCheck.execute(keys, values);

      tm.resume(tx);
      tm.commit();

      dataCheck.execute(keys, values, false);
   }

   private void empty(List<K> keys, List<V> values) {
   }

   private void initKeys(List<K> keys, List<V> values) {
      RemoteCache<K, V> cache = txRemoteCache();
      for (int i = 0; i < keys.size(); ++i) {
         cache.put(keys.get(i), values.get(i));
      }
   }

   private void checkNoKeys(List<K> keys, List<V> values) {
      checkNoKeys(keys, values, true);
   }

   private void checkNoKeys(List<K> keys, List<V> values, boolean inTx) {
      RemoteCache<K, V> cache = txRemoteCache();
      for (K key : keys) {
         assertNull(cache.get(key));
      }
   }

   private void checkInitValue(List<K> keys, List<V> values) {
      checkInitValue(keys, values, true);
   }

   private void checkInitValue(List<K> keys, List<V> values, boolean inTx) {
      RemoteCache<K, V> cache = txRemoteCache();
      for (int i = 0; i < keys.size(); ++i) {
         kvGenerator.assertValueEquals(values.get(i), cache.get(keys.get(i)));
      }
   }

   private void secondHalfDataCheck(List<K> keys, List<V> values, boolean inTx) {
      putDataCheck(keys, values.subList(keys.size(), values.size()), inTx);
   }

   private void put(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(3, keys.size());
      assertEquals(3, values.size());
      RemoteCache<K, V> cache = txRemoteCache();

      if (sync) {
         cache.put(keys.get(0), values.get(0));
         cache.put(keys.get(1), values.get(1), 1, TimeUnit.MINUTES);
         cache.put(keys.get(2), values.get(2), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES);
      } else {
         cache.putAsync(keys.get(0), values.get(0)).get();
         cache.putAsync(keys.get(1), values.get(1), 1, TimeUnit.MINUTES).get();
         cache.putAsync(keys.get(2), values.get(2), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES).get();
      }
   }

   private void putIfAbsent(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(3, keys.size());
      assertEquals(3, values.size());
      RemoteCache<K, V> cache = txRemoteCache();

      if (sync) {
         cache.putIfAbsent(keys.get(0), values.get(0));
         cache.putIfAbsent(keys.get(1), values.get(1), 1, TimeUnit.MINUTES);
         cache.putIfAbsent(keys.get(2), values.get(2), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES);
      } else {
         cache.putIfAbsentAsync(keys.get(0), values.get(0)).get();
         cache.putIfAbsentAsync(keys.get(1), values.get(1), 1, TimeUnit.MINUTES).get();
         cache.putIfAbsentAsync(keys.get(2), values.get(2), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES).get();
      }
   }

   private void putDataCheck(List<K> keys, List<V> values, boolean inTx) {
      RemoteCache<K, V> cache = txRemoteCache();
      kvGenerator.assertValueEquals(values.get(0), cache.get(keys.get(0)));

      //max idle is zero. that means to use the default server value
      //outside the transaction, it returns -1 for infinite max-idle
      assertMetadataValue(values.get(1), cache.getWithMetadata(keys.get(1)), TimeUnit.MINUTES.toSeconds(1),
            inTx ? 0 : -1);
      assertMetadataValue(values.get(2), cache.getWithMetadata(keys.get(2)), TimeUnit.MINUTES.toSeconds(2),
            TimeUnit.MINUTES.toSeconds(3));
   }

   private void putAll(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(6, keys.size());
      assertEquals(6, values.size());
      RemoteCache<K, V> cache = txRemoteCache();

      if (sync) {
         Map<K, V> map = new HashMap<>();
         map.put(keys.get(0), values.get(0));
         map.put(keys.get(1), values.get(1));
         cache.putAll(map);

         map.clear();
         map.put(keys.get(2), values.get(2));
         map.put(keys.get(3), values.get(3));
         cache.putAll(map, 1, TimeUnit.MINUTES);

         map.clear();
         map.put(keys.get(4), values.get(4));
         map.put(keys.get(5), values.get(5));
         cache.putAll(map, 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES);
      } else {
         Map<K, V> map = new HashMap<>();
         map.put(keys.get(0), values.get(0));
         map.put(keys.get(1), values.get(1));
         cache.putAllAsync(map).get();

         map.clear();
         map.put(keys.get(2), values.get(2));
         map.put(keys.get(3), values.get(3));
         cache.putAllAsync(map, 1, TimeUnit.MINUTES).get();

         map.clear();
         map.put(keys.get(4), values.get(4));
         map.put(keys.get(5), values.get(5));
         cache.putAllAsync(map, 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES).get();
      }
   }

   private void putAllDataCheck(List<K> keys, List<V> values, boolean inTx) {
      RemoteCache<K, V> cache = txRemoteCache();
      kvGenerator.assertValueEquals(values.get(0), cache.get(keys.get(0)));
      kvGenerator.assertValueEquals(values.get(1), cache.get(keys.get(1)));

      //max idle is zero. that means to use the default server value
      assertMetadataValue(values.get(2), cache.getWithMetadata(keys.get(2)), TimeUnit.MINUTES.toSeconds(1),
            inTx ? 0 : -1);
      assertMetadataValue(values.get(3), cache.getWithMetadata(keys.get(3)), TimeUnit.MINUTES.toSeconds(1),
            inTx ? 0 : -1);

      assertMetadataValue(values.get(4), cache.getWithMetadata(keys.get(4)), TimeUnit.MINUTES.toSeconds(2),
            TimeUnit.MINUTES.toSeconds(3));
      assertMetadataValue(values.get(5), cache.getWithMetadata(keys.get(5)), TimeUnit.MINUTES.toSeconds(2),
            TimeUnit.MINUTES.toSeconds(3));
   }

   private void replace(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(6, keys.size());
      assertEquals(12, values.size());
      RemoteCache<K, V> cache = txRemoteCache();

      if (sync) {
         kvGenerator.assertValueEquals(values.get(0), cache.replace(keys.get(0), values.get(6)));
         kvGenerator.assertValueEquals(values.get(1), cache.replace(keys.get(1), values.get(7), 1, TimeUnit.MINUTES));
         kvGenerator.assertValueEquals(values.get(2),
               cache.replace(keys.get(2), values.get(8), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES));
         assertTrue(cache.replace(keys.get(3), values.get(3), values.get(9)));
         assertTrue(cache.replace(keys.get(4), values.get(4), values.get(10), 4, TimeUnit.MINUTES));
         assertTrue(
               cache.replace(keys.get(5), values.get(5), values.get(11), 5, TimeUnit.MINUTES, 6, TimeUnit.MINUTES));
      } else {
         kvGenerator.assertValueEquals(values.get(0), cache.replaceAsync(keys.get(0), values.get(6)).get());
         kvGenerator.assertValueEquals(values.get(1),
               cache.replaceAsync(keys.get(1), values.get(7), 1, TimeUnit.MINUTES).get());
         kvGenerator.assertValueEquals(values.get(2),
               cache.replaceAsync(keys.get(2), values.get(8), 2, TimeUnit.MINUTES, 3, TimeUnit.MINUTES).get());

         //async methods aren't supported in the original cache
         expectException(UnsupportedOperationException.class,
               () -> cache.replaceAsync(keys.get(3), values.get(3), values.get(9)));
         expectException(UnsupportedOperationException.class,
               () -> cache.replaceAsync(keys.get(4), values.get(4), values.get(10), 4, TimeUnit.MINUTES));
         expectException(UnsupportedOperationException.class, () -> cache
               .replaceAsync(keys.get(5), values.get(5), values.get(11), 5, TimeUnit.MINUTES, 6, TimeUnit.MINUTES));

         //the test still expects this to be replaced.
         assertTrue(cache.replace(keys.get(3), values.get(3), values.get(9)));
         assertTrue(cache.replace(keys.get(4), values.get(4), values.get(10), 4, TimeUnit.MINUTES));
         assertTrue(
               cache.replace(keys.get(5), values.get(5), values.get(11), 5, TimeUnit.MINUTES, 6, TimeUnit.MINUTES));
      }
   }

   private void replaceWithVersion(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(4, keys.size());
      assertEquals(8, values.size());
      RemoteCache<K, V> cache = txRemoteCache();
      if (sync) {
         MetadataValue<V> value = cache.getWithMetadata(keys.get(0));
         assertTrue(cache.replaceWithVersion(keys.get(0), values.get(4), value.getVersion()));

         value = cache.getWithMetadata(keys.get(1));
         assertTrue(cache.replaceWithVersion(keys.get(1), values.get(5), value.getVersion(), 60));

         value = cache.getWithMetadata(keys.get(2));
         assertTrue(cache.replaceWithVersion(keys.get(2), values.get(6), value.getVersion(), 120, 180));

         value = cache.getWithMetadata(keys.get(3));
         assertTrue(cache.replaceWithVersion(keys.get(3), values.get(7), value.getVersion(), 4, TimeUnit.MINUTES, 5,
               TimeUnit.MINUTES));
      } else {
         MetadataValue<V> value = cache.getWithMetadata(keys.get(0));
         assertTrue(cache.replaceWithVersionAsync(keys.get(0), values.get(4), value.getVersion()).get());

         value = cache.getWithMetadata(keys.get(1));
         assertTrue(cache.replaceWithVersionAsync(keys.get(1), values.get(5), value.getVersion(), 60).get());

         value = cache.getWithMetadata(keys.get(2));
         assertTrue(cache.replaceWithVersionAsync(keys.get(2), values.get(6), value.getVersion(), 120, 180).get());

         value = cache.getWithMetadata(keys.get(3));
         assertTrue(cache.replaceWithVersion(keys.get(3), values.get(7), value.getVersion(), 4, TimeUnit.MINUTES, 5,
               TimeUnit.MINUTES));
      }
   }

   private void remove(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(2, keys.size());
      assertEquals(2, values.size());
      RemoteCache<K, V> cache = txRemoteCache();
      if (sync) {
         cache.remove(keys.get(0));
         assertTrue(cache.remove(keys.get(1), values.get(1)));
      } else {
         cache.removeAsync(keys.get(0)).get();

         //not supported
         expectException(UnsupportedOperationException.class, () -> cache.removeAsync(keys.get(1), values.get(1)));

         cache.remove(keys.get(1));
      }
   }

   private void removeWithVersion(List<K> keys, List<V> values, boolean sync) throws Exception {
      assertEquals(1, keys.size());
      assertEquals(1, values.size());
      RemoteCache<K, V> cache = txRemoteCache();
      if (sync) {
         MetadataValue<V> value = cache.getWithMetadata(keys.get(0));
         assertTrue(cache.removeWithVersion(keys.get(0), value.getVersion()));
      } else {
         MetadataValue<V> value = cache.getWithMetadata(keys.get(0));
         assertTrue(cache.removeWithVersionAsync(keys.get(0), value.getVersion()).get());
      }
   }

   private void compute(List<K> keys, List<V> values, boolean sync) {
      assertEquals(1, keys.size());
      assertEquals(1, values.size());
      assertTrue(sync);
      RemoteCache<K, V> cache = txRemoteCache();
      cache.compute(keys.get(0), (k, v) -> values.get(0));
   }

   private void computeIfAbsent(List<K> keys, List<V> values, boolean sync) {
      assertEquals(1, keys.size());
      assertEquals(1, values.size());
      assertTrue(sync);
      RemoteCache<K, V> cache = txRemoteCache();
      cache.computeIfAbsent(keys.get(0), k -> values.get(0));
   }

   private void computeIfPresent(List<K> keys, List<V> values, boolean sync) {
      assertEquals(1, keys.size());
      assertEquals(2, values.size());
      assertTrue(sync);
      RemoteCache<K, V> cache = txRemoteCache();
      cache.compute(keys.get(0), (k, v) -> values.get(1));
   }

   private void assertMetadataValue(V expected, MetadataValue<V> value, long lifespan, long maxIdle) {
      assertNotNull(value);
      kvGenerator.assertValueEquals(expected, value.getValue());
      assertEquals(lifespan, value.getLifespan());
      assertEquals(maxIdle, value.getMaxIdle());
   }

   private RemoteCache<K, V> txRemoteCache() {
      return client(0).getCache(CACHE_NAME);
   }

   private List<K> generateKeys(Method method, int count) {
      List<K> keys = new ArrayList<>(count);
      for (int i = 0; i < count; ++i) {
         keys.add(kvGenerator.generateKey(method, i));
      }
      return keys;
   }

   private List<V> generateValues(Method method, int count) {
      List<V> keys = new ArrayList<>(count);
      for (int i = 0; i < count; ++i) {
         keys.add(kvGenerator.generateValue(method, i));
      }
      return keys;
   }

   private interface DataCheck<K, V> {
      void execute(List<K> keys, List<V> values, boolean inTx);
   }

   private interface Step<K, V> {
      void execute(List<K> keys, List<V> values);
   }

   private interface OperationStep<K, V> {
      void execute(List<K> keys, List<V> values, boolean sync) throws Exception;
   }

}
