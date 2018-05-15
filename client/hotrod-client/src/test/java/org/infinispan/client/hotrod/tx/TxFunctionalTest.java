package org.infinispan.client.hotrod.tx;

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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Simple Functional test.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.tx.TxFunctionalTest")
public class TxFunctionalTest<K, V> extends MultiHotRodServersTest {

   private KeyValueGenerator<K, V> kvGenerator;
   private TransactionMode transactionMode;

   public TxFunctionalTest<K, V> keyValueGenerator(KeyValueGenerator<K, V> kvGenerator) {
      this.kvGenerator = kvGenerator;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new TxFunctionalTest<String, String>()
                  .keyValueGenerator(KeyValueGenerator.STRING_GENERATOR).transactionMode(TransactionMode.NON_XA),
            new TxFunctionalTest<byte[], byte[]>()
                  .keyValueGenerator(KeyValueGenerator.BYTE_ARRAY_GENERATOR).transactionMode(TransactionMode.NON_XA),
            new TxFunctionalTest<Object[], Object[]>()
                  .keyValueGenerator(KeyValueGenerator.GENERIC_ARRAY_GENERATOR).transactionMode(TransactionMode.NON_XA)
      };
   }

   public TxFunctionalTest<K, V> transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   @BeforeClass(alwaysRun = true)
   public void printParameters() {
      log.debugf("Parameters: %s", super.parameters());
   }

   public void testSimpleTransaction(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCache(0);
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCache.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v1));
      tm.commit();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, v1);

      //test without tx
      remoteCache.put(k1, v2);
      remoteCache.put(k2, v2);

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, v2);
   }

   public void testTransactionIsolation(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCache(0);
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCache.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);

      tm.begin();
      remoteCache.put(k1, v2);
      remoteCache.put(k2, v2);
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      tm.commit();

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, v2);

      tm.resume(tx1);
      //it shouldn't see the other transaction updates!
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      tm.commit();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, v1);
   }

   public void testRollback(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);

      RemoteCache<K, V> remoteCache = remoteCache(0);
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCache.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      tm.rollback();

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);
   }

   public void testSetAsRollback(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);

      RemoteCache<K, V> remoteCache = remoteCache(0);
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCache.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      tm.setRollbackOnly();
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);
   }

   public void testConflictWithUpdateNonExisting(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCacheWithForceReturnValue();
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCache.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);

      remoteCache.put(k1, v2);

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, null);

      tm.resume(tx1);
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, null);
   }

   public void testConflictWithTxUpdateNonExisting(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCacheWithForceReturnValue();
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(null, remoteCache.put(k1, v1));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);

      tm.begin();
      remoteCache.put(k1, v2);
      tm.commit();

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, null);

      tm.resume(tx1);
      kvGenerator.assertValueEquals(v1, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v1, remoteCache.get(k2));
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, null);
   }

   public void testConflictWithUpdate(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCacheWithForceReturnValue();
      final TransactionManager tm = remoteCache.getTransactionManager();

      remoteCache.put(k1, v1);

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(v1, remoteCache.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v2));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);

      remoteCache.put(k1, v1);

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);

      tm.resume(tx1);
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);
   }

   public void testConflictWithTxUpdate(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCacheWithForceReturnValue();
      final TransactionManager tm = remoteCache.getTransactionManager();

      remoteCache.put(k1, v1);

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(v1, remoteCache.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v2));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);

      tm.begin();
      remoteCache.put(k1, v1);
      tm.commit();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);

      tm.resume(tx1);
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);
   }

   public void testConflictWithRemove(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCacheWithForceReturnValue();
      final TransactionManager tm = remoteCache.getTransactionManager();

      remoteCache.put(k1, v1);

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(v1, remoteCache.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v2));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);

      remoteCache.remove(k1);

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);

      tm.resume(tx1);
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);
   }

   public void testConflictWithTxRemove(Method method) throws Exception {
      final K k1 = kvGenerator.generateKey(method, 1);
      final K k2 = kvGenerator.generateKey(method, 2);
      final V v1 = kvGenerator.generateValue(method, 1);
      final V v2 = kvGenerator.generateValue(method, 2);

      RemoteCache<K, V> remoteCache = remoteCacheWithForceReturnValue();
      final TransactionManager tm = remoteCache.getTransactionManager();

      remoteCache.put(k1, v1);

      //test with tx
      tm.begin();
      kvGenerator.assertValueEquals(v1, remoteCache.put(k1, v2));
      kvGenerator.assertValueEquals(null, remoteCache.put(k2, v2));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      final Transaction tx1 = tm.suspend();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, null);

      tm.begin();
      remoteCache.remove(k1);
      tm.commit();

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);

      tm.resume(tx1);
      kvGenerator.assertValueEquals(v2, remoteCache.get(k1));
      kvGenerator.assertValueEquals(v2, remoteCache.get(k2));
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEntryInAllClients(k1, null);
      assertEntryInAllClients(k2, null);
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

   protected String cacheName() {
      return "tx-cache";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cacheBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cacheBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      createHotRodServers(numberOfNodes(), new ConfigurationBuilder());
      defineInAll(cacheName(), cacheBuilder);
   }

   protected final RemoteCache<K, V> remoteCache(int index) {
      return client(index).getCache(cacheName());
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

   private int numberOfNodes() {
      return 3;
   }

   private RemoteCache<K, V> remoteCacheWithForceReturnValue() {
      return client(0).getCache(cacheName(), true);
   }

   private void assertEntryInAllClients(K key, V value) {
      for (RemoteCacheManager manager : clients) {
         RemoteCache<K, V> remoteCache = manager.getCache(cacheName());
         kvGenerator.assertValueEquals(value, remoteCache.get(key));
      }
   }
}
