package org.infinispan.client.hotrod.tx;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.assertNoTransaction;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.Collection;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.client.hotrod.tx.util.KeyValueGenerator;
import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Last Resource Commit Optimization test (1PC)
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.tx.LCROTest")
public class LCROTest extends MultiHotRodServersTest {
   private static final String CACHE_A = "lrco-a";
   private static final String CACHE_B = "lrco-b";
   private static final String CACHE_C = "lrco-c";
   private static final KeyValueGenerator<String, String> GENERATOR = KeyValueGenerator.STRING_GENERATOR;


   public void testFailureInA(Method method) throws Exception {
      doSingleTestFailure(method, CACHE_A);
   }

   public void testFailureInB(Method method) throws Exception {
      doSingleTestFailure(method, CACHE_B);
   }

   public void testFailureInC(Method method) throws Exception {
      doSingleTestFailure(method, CACHE_C);
   }

   public void testReadOnlyWithWriteInA(Method method) throws Exception {
      doReadOnlyWithSingleWriteTest(method, CACHE_A, false);
   }

   public void testReadOnlyWithFailedWriteInA(Method method) throws Exception {
      doReadOnlyWithSingleWriteTest(method, CACHE_A, true);
   }

   public void testReadOnlyWithWriteInB(Method method) throws Exception {
      doReadOnlyWithSingleWriteTest(method, CACHE_B, false);
   }

   public void testReadOnlyWithFailedWriteInB(Method method) throws Exception {
      doReadOnlyWithSingleWriteTest(method, CACHE_B, true);
   }

   public void testReadOnlyWithWriteInC(Method method) throws Exception {
      doReadOnlyWithSingleWriteTest(method, CACHE_C, false);
   }

   public void testReadOnlyWithFailedWriteInC(Method method) throws Exception {
      doReadOnlyWithSingleWriteTest(method, CACHE_C, true);
   }

   public void testReadOnly(Method method) throws Exception {
      final String key = GENERATOR.generateKey(method, 0);
      final String value1 = GENERATOR.generateValue(method, 0);

      final RemoteCache<String, String> cacheA = client(0).getCache(CACHE_A);
      final RemoteCache<String, String> cacheB = client(0).getCache(CACHE_B);
      final RemoteCache<String, String> cacheC = client(0).getCache(CACHE_C);

      cacheA.put(key, value1);
      cacheB.put(key, value1);
      cacheC.put(key, value1);

      final TransactionManager tm = cacheA.getTransactionManager();
      tm.begin();
      GENERATOR.assertValueEquals(value1, cacheA.get(key));
      GENERATOR.assertValueEquals(value1, cacheB.get(key));
      GENERATOR.assertValueEquals(value1, cacheC.get(key));
      final TransactionImpl tx = (TransactionImpl) tm.suspend();

      XAResource resource = extractXaResource(tx);
      //just to be sure that we don't have any NPE or similar exception when there are no writes
      resource.commit(tx.getXid(), true); //force 1PC
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
      //force the return value
      clientBuilder.forceReturnValues(true);
      //use our TM to test it to extract the XaResource.
      clientBuilder.transaction().transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());
      //only for XA modes
      clientBuilder.transaction().transactionMode(TransactionMode.NON_DURABLE_XA);
      return clientBuilder;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      assertNoTransaction(clients);
      super.clearContent();
   }

   private void doSingleTestFailure(Method method, String conflictCacheName) throws Exception {
      final String key = GENERATOR.generateKey(method, 0);
      final String value1 = GENERATOR.generateValue(method, 0);
      final String value2 = GENERATOR.generateValue(method, 1);

      final RemoteCache<String, String> cacheA = client(0).getCache(CACHE_A);
      final RemoteCache<String, String> cacheB = client(0).getCache(CACHE_B);
      final RemoteCache<String, String> cacheC = client(0).getCache(CACHE_C);

      final TransactionManager tm = cacheA.getTransactionManager();
      tm.begin();
      cacheA.put(key, value1);
      cacheB.put(key, value1);
      cacheC.put(key, value1);
      final TransactionImpl tx = (TransactionImpl) tm.suspend();

      client(0).getCache(conflictCacheName).put(key, value2); //creates a conflict with the tx.

      XAResource resource = extractXaResource(tx);
      try {
         resource.commit(tx.getXid(), true); //force 1PC
         fail("Rollback is expected");
      } catch (XAException e) {
         assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
      }

      GENERATOR.assertValueEquals(CACHE_A.equals(conflictCacheName) ? value2 : null, cacheA.get(key));
      GENERATOR.assertValueEquals(CACHE_B.equals(conflictCacheName) ? value2 : null, cacheB.get(key));
      GENERATOR.assertValueEquals(CACHE_C.equals(conflictCacheName) ? value2 : null, cacheC.get(key));
   }

   private void doReadOnlyWithSingleWriteTest(Method method, String writeCache, boolean rollback) throws Exception {
      final String key = GENERATOR.generateKey(method, 0);
      final String value1 = GENERATOR.generateValue(method, 0);
      final String value2 = GENERATOR.generateValue(method, 1);
      final String value3 = GENERATOR.generateValue(method, 2);

      final RemoteCache<String, String> cacheA = client(0).getCache(CACHE_A);
      final RemoteCache<String, String> cacheB = client(0).getCache(CACHE_B);
      final RemoteCache<String, String> cacheC = client(0).getCache(CACHE_C);

      cacheA.put(key, value1);
      cacheB.put(key, value1);
      cacheC.put(key, value1);

      final TransactionManager tm = cacheA.getTransactionManager();
      tm.begin();
      GENERATOR.assertValueEquals(value1, cacheA.get(key));
      GENERATOR.assertValueEquals(value1, cacheB.get(key));
      GENERATOR.assertValueEquals(value1, cacheC.get(key));

      client(0).getCache(writeCache).put(key, value2);

      final TransactionImpl tx = (TransactionImpl) tm.suspend();

      if (rollback) {
         client(0).getCache(writeCache).put(key, value3); //creates a conflict with the tx.
      }

      XAResource resource = extractXaResource(tx);
      try {
         resource.commit(tx.getXid(), true); //force 1PC
         assertFalse(rollback);
      } catch (XAException e) {
         assertTrue(rollback);
         assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
      }

      GENERATOR.assertValueEquals(CACHE_A.equals(writeCache) ? (rollback ? value3 : value2) : value1, cacheA.get(key));
      GENERATOR.assertValueEquals(CACHE_B.equals(writeCache) ? (rollback ? value3 : value2) : value1, cacheB.get(key));
      GENERATOR.assertValueEquals(CACHE_C.equals(writeCache) ? (rollback ? value3 : value2) : value1, cacheC.get(key));

   }

   private XAResource extractXaResource(TransactionImpl tx) {
      Collection<XAResource> resources = tx.getEnlistedResources();
      assertEquals(1, resources.size());
      return resources.iterator().next();
   }
}
