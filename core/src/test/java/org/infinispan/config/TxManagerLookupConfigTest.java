package org.infinispan.config;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.TxManagerLookupConfigTest")
public class TxManagerLookupConfigTest {

   static TmA tma = new TmA();
   static TmB tmb = new TmB();

   public void simpleTest() {
      final DefaultCacheManager cacheManager = new DefaultCacheManager(GlobalConfiguration.getNonClusteredDefault(), new Configuration(), true);

      Configuration customConfiguration = new Configuration();
      customConfiguration.setTransactionManagerLookup(new TxManagerLookupA());
      Configuration definedConfiguration = cacheManager.defineConfiguration("aCache", customConfiguration);

      // verify the setting was not lost:
      assertTrue(definedConfiguration.getTransactionManagerLookup() instanceof TxManagerLookupA);

      // verify it's actually being used:
      TransactionManager activeTransactionManager = cacheManager.getCache("aCache").getAdvancedCache().getTransactionManager();
      assertNotNull(activeTransactionManager);
      assertTrue(activeTransactionManager instanceof TmA);
   }

   public static class TmA extends DummyTransactionManager {}

   public static class TmB extends DummyTransactionManager {}

   public static class TxManagerLookupA implements TransactionManagerLookup {

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return tma;
      }
   }

   public static class TxManagerLookupB implements TransactionManagerLookup {

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return tmb;
      }
   }
}
