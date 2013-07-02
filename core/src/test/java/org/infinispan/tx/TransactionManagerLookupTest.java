package org.infinispan.tx;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * Tests all TransactionManagerLookup impls shipped with Infinispan for correctness
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test(testName = "tx.TransactionManagerLookupTest", groups = "unit")
public class TransactionManagerLookupTest extends AbstractInfinispanTest {
   
   Configuration configuration = new ConfigurationBuilder().build();

   public void testGenericTransactionManagerLookup() throws Exception {
      GenericTransactionManagerLookup lookup = new GenericTransactionManagerLookup();
      lookup.setConfiguration(configuration);
      doTest(lookup);
   }

   public void testDummyTransactionManagerLookup() throws Exception {
      doTest(new DummyTransactionManagerLookup());
   }

   public void testJBossStandaloneJTAManagerLookup() throws Exception {
      JBossStandaloneJTAManagerLookup lookup = new JBossStandaloneJTAManagerLookup();
      lookup.init(configuration);
      doTest(lookup);
   }
   
   protected void doTest(TransactionManagerLookup lookup) throws Exception {
      TransactionManager tm = lookup.getTransactionManager();
      tm.begin();
      tm.commit();

      tm.begin();
      tm.rollback();
   }

}
