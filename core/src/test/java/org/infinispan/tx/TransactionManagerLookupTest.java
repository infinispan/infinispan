package org.infinispan.tx;

import javax.transaction.TransactionManager;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * Tests all TransactionManagerLookup impls shipped with Infinispan for correctness
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test(testName = "tx.TransactionManagerLookupTest", groups = "unit")
public class TransactionManagerLookupTest extends AbstractInfinispanTest {

   final GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();

   public void testGenericTransactionManagerLookup() throws Exception {
      GenericTransactionManagerLookup lookup = new GenericTransactionManagerLookup();
      TestingUtil.inject(lookup, globalConfiguration);
      doTest(lookup);
   }

   public void testDummyTransactionManagerLookup() throws Exception {
      doTest(new EmbeddedTransactionManagerLookup());
   }

   public void testJBossStandaloneJTAManagerLookup() throws Exception {
      JBossStandaloneJTAManagerLookup lookup = new JBossStandaloneJTAManagerLookup();
      lookup.init(globalConfiguration);
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
