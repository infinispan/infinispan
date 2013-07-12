package org.infinispan.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;


import javax.transaction.TransactionManager;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.TxManagerLookupConfigTest")
public class TxManagerLookupConfigTest {

   static TmA tma = new TmA();
   static TmB tmb = new TmB();

   public void simpleTest() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()){
         @Override
         public void call() {
            ConfigurationBuilder customConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
            customConfiguration.transaction().transactionManagerLookup(new TxManagerLookupA());
            Configuration definedConfiguration = cm.defineConfiguration("aCache", customConfiguration.build());

            // verify the setting was not lost:
            assertTrue(definedConfiguration.transaction().transactionManagerLookup() instanceof TxManagerLookupA);

            // verify it's actually being used:
            TransactionManager activeTransactionManager = cm.getCache("aCache").getAdvancedCache().getTransactionManager();
            assertNotNull(activeTransactionManager);
            assertTrue(activeTransactionManager instanceof TmA);
         }
      });
   }

   public static class TmA extends DummyTransactionManager {}

   public static class TmB extends DummyTransactionManager {}

   public static class TxManagerLookupA implements TransactionManagerLookup {

      @Override
      public synchronized TransactionManager getTransactionManager() throws Exception {
         return tma;
      }
   }

   public static class TxManagerLookupB implements TransactionManagerLookup {

      @Override
      public synchronized TransactionManager getTransactionManager() throws Exception {
         return tmb;
      }
   }
}
