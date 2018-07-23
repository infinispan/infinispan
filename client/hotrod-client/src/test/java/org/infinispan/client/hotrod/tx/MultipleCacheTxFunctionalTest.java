package org.infinispan.client.hotrod.tx;

import static org.infinispan.client.hotrod.configuration.TransactionMode.FULL_XA;
import static org.infinispan.client.hotrod.configuration.TransactionMode.NON_DURABLE_XA;
import static org.infinispan.client.hotrod.configuration.TransactionMode.NON_XA;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.assertNoTransaction;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.BYTE_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.GENERIC_ARRAY_GENERATOR;
import static org.infinispan.client.hotrod.tx.util.KeyValueGenerator.STRING_GENERATOR;
import static org.testng.AssertJUnit.assertSame;

import java.lang.reflect.Method;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.tx.util.KeyValueGenerator;
import org.infinispan.client.hotrod.tx.util.TransactionSetup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests transactions involving multiple {@link RemoteCache}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.tx.MultipleCacheTxFunctionalTest")
public class MultipleCacheTxFunctionalTest<K, V> extends MultiHotRodServersTest {
   private static final String CACHE_A = "tx-cache-a";
   private static final String CACHE_B = "tx-cache-b";
   private static final String CACHE_C = "tx-cache-c";
   private KeyValueGenerator<K, V> kvGenerator;
   private TransactionMode transactionMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            new MultipleCacheTxFunctionalTest<String, String>().keyValueGenerator(STRING_GENERATOR).transactionMode(NON_XA),
            new MultipleCacheTxFunctionalTest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).transactionMode(NON_XA),
            new MultipleCacheTxFunctionalTest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).transactionMode(NON_XA),
            new MultipleCacheTxFunctionalTest<String, String>().keyValueGenerator(STRING_GENERATOR).transactionMode(NON_DURABLE_XA),
            new MultipleCacheTxFunctionalTest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).transactionMode(NON_DURABLE_XA),
            new MultipleCacheTxFunctionalTest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).transactionMode(NON_DURABLE_XA),
            new MultipleCacheTxFunctionalTest<String, String>().keyValueGenerator(STRING_GENERATOR).transactionMode(FULL_XA),
            new MultipleCacheTxFunctionalTest<byte[], byte[]>().keyValueGenerator(BYTE_ARRAY_GENERATOR).transactionMode(FULL_XA),
            new MultipleCacheTxFunctionalTest<Object[], Object[]>().keyValueGenerator(GENERIC_ARRAY_GENERATOR).transactionMode(FULL_XA)
      };
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      assertNoTransaction(clients);
      super.clearContent();
   }

   public void testMultipleCaches(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final K k3 = kvGenerator.generateKey(method, 3);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);
      final V v3 = kvGenerator.generateValue(method, 3);

      RemoteCache<K, V> remoteCacheA = remoteCache(CACHE_A);
      RemoteCache<K, V> remoteCacheB = remoteCache(CACHE_B);
      RemoteCache<K, V> remoteCacheC = remoteCache(CACHE_C);

      assertSame(remoteCacheA.getTransactionManager(), remoteCacheB.getTransactionManager());
      assertSame(remoteCacheA.getTransactionManager(), remoteCacheC.getTransactionManager());

      final TransactionManager tm = remoteCacheA.getTransactionManager();
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCacheA.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCacheB.put(k2, v2));
      kvGenerator.assertValueEquals(null, remoteCacheC.put(k3, v3));
      kvGenerator.assertValueEquals(v1, remoteCacheA.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCacheB.get(k2));
      kvGenerator.assertValueEquals(v3, remoteCacheC.get(k3));
      tm.commit();

      assertEntryInAllClients(CACHE_A, k1, v1);
      assertEntryInAllClients(CACHE_A, k2, null);
      assertEntryInAllClients(CACHE_A, k3, null);
      assertEntryInAllClients(CACHE_B, k1, null);
      assertEntryInAllClients(CACHE_B, k2, v2);
      assertEntryInAllClients(CACHE_B, k3, null);
      assertEntryInAllClients(CACHE_C, k1, null);
      assertEntryInAllClients(CACHE_C, k2, null);
      assertEntryInAllClients(CACHE_C, k3, v3);
   }

   public void testMultipleCacheWithSameKey(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);
      final V v3 = kvGenerator.generateValue(method, 3);

      RemoteCache<K, V> remoteCacheA = remoteCache(CACHE_A);
      RemoteCache<K, V> remoteCacheB = remoteCache(CACHE_B);
      RemoteCache<K, V> remoteCacheC = remoteCache(CACHE_C);

      assertSame(remoteCacheA.getTransactionManager(), remoteCacheB.getTransactionManager());
      assertSame(remoteCacheA.getTransactionManager(), remoteCacheC.getTransactionManager());

      final TransactionManager tm = remoteCacheA.getTransactionManager();
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCacheA.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCacheB.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCacheC.put(k1, v3));
      kvGenerator.assertValueEquals(v1, remoteCacheA.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCacheB.get(k1));
      kvGenerator.assertValueEquals(v3, remoteCacheC.get(k1));
      tm.commit();

      assertEntryInAllClients(CACHE_A, k1, v1);
      assertEntryInAllClients(CACHE_B, k1, v2);
      assertEntryInAllClients(CACHE_C, k1, v3);
   }

   public void testMultipleCacheWithConflict(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);
      final V v3 = kvGenerator.generateValue(method, 3);
      final V v4 = kvGenerator.generateValue(method, 4);

      RemoteCache<K, V> remoteCacheA = remoteCache(CACHE_A);
      RemoteCache<K, V> remoteCacheB = remoteCache(CACHE_B);
      RemoteCache<K, V> remoteCacheC = remoteCache(CACHE_C);

      assertSame(remoteCacheA.getTransactionManager(), remoteCacheB.getTransactionManager());
      assertSame(remoteCacheA.getTransactionManager(), remoteCacheC.getTransactionManager());

      final TransactionManager tm = remoteCacheA.getTransactionManager();
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCacheA.get(k1));
      kvGenerator.assertValueEquals(null, remoteCacheA.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCacheB.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCacheC.put(k1, v3));
      kvGenerator.assertValueEquals(v1, remoteCacheA.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCacheB.get(k1));
      kvGenerator.assertValueEquals(v3, remoteCacheC.get(k1));
      final Transaction tx = tm.suspend();

      client(0).getCache(CACHE_A).put(k1, v4);

      tm.resume(tx);
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(CACHE_A, k1, v4);
      assertEntryInAllClients(CACHE_B, k1, null);
      assertEntryInAllClients(CACHE_C, k1, null);
   }

   public void testMultipleCacheRollback(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);
      final V v3 = kvGenerator.generateValue(method, 3);


      RemoteCache<K, V> remoteCacheA = remoteCache(CACHE_A);
      RemoteCache<K, V> remoteCacheB = remoteCache(CACHE_B);
      RemoteCache<K, V> remoteCacheC = remoteCache(CACHE_C);

      assertSame(remoteCacheA.getTransactionManager(), remoteCacheB.getTransactionManager());
      assertSame(remoteCacheA.getTransactionManager(), remoteCacheC.getTransactionManager());

      final TransactionManager tm = remoteCacheA.getTransactionManager();
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCacheA.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCacheB.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCacheC.put(k1, v3));
      kvGenerator.assertValueEquals(v1, remoteCacheA.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCacheB.get(k1));
      kvGenerator.assertValueEquals(v3, remoteCacheC.get(k1));
      tm.rollback();

      assertEntryInAllClients(CACHE_A, k1, null);
      assertEntryInAllClients(CACHE_B, k1, null);
      assertEntryInAllClients(CACHE_C, k1, null);
   }

   public void testMultipleCacheSetRollbackOnly(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);
      final V v3 = kvGenerator.generateValue(method, 3);


      RemoteCache<K, V> remoteCacheA = remoteCache(CACHE_A);
      RemoteCache<K, V> remoteCacheB = remoteCache(CACHE_B);
      RemoteCache<K, V> remoteCacheC = remoteCache(CACHE_C);

      assertSame(remoteCacheA.getTransactionManager(), remoteCacheB.getTransactionManager());
      assertSame(remoteCacheA.getTransactionManager(), remoteCacheC.getTransactionManager());

      final TransactionManager tm = remoteCacheA.getTransactionManager();
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCacheA.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCacheB.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCacheC.put(k1, v3));
      kvGenerator.assertValueEquals(v1, remoteCacheA.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCacheB.get(k1));
      kvGenerator.assertValueEquals(v3, remoteCacheC.get(k1));
      tm.setRollbackOnly();
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(CACHE_A, k1, null);
      assertEntryInAllClients(CACHE_B, k1, null);
      assertEntryInAllClients(CACHE_C, k1, null);
   }

   @BeforeClass(alwaysRun = true)
   public void printParameters() {
      log.debugf("Parameters: %s", super.parameters());
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
      cacheBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cacheBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      createHotRodServers(3, new ConfigurationBuilder());
      defineInAll(CACHE_A, cacheBuilder);
      defineInAll(CACHE_B, cacheBuilder);
      defineInAll(CACHE_C, cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
         String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
            .createHotRodClientConfigurationBuilder(host, serverPort);
      clientBuilder.forceReturnValues(false);
      TransactionSetup.amendJTA(clientBuilder);
      clientBuilder.transaction().transactionMode(transactionMode);
      return clientBuilder;
   }

   private RemoteCache<K, V> remoteCache(String cacheName) {
      return client(0).getCache(cacheName);
   }

   private void assertEntryInAllClients(String cacheName, K key, V value) {
      for (RemoteCacheManager manager : clients) {
         RemoteCache<K, V> remoteCache = manager.getCache(cacheName);
         kvGenerator.assertValueEquals(value, remoteCache.get(key));
      }
   }

   private MultipleCacheTxFunctionalTest<K, V> keyValueGenerator(KeyValueGenerator<K, V> kvGenerator) {
      this.kvGenerator = kvGenerator;
      return this;
   }

   private MultipleCacheTxFunctionalTest<K, V> transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }
}
