package org.infinispan.tx;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.lookup.JBossTransactionManagerLookup;
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

   public void testGenericTransactionManagerLookup() throws Exception {
      doTest(new GenericTransactionManagerLookup());
   }

   public void testDummyTransactionManagerLookup() throws Exception {
      doTest(new DummyTransactionManagerLookup());
   }

   public void testJBossStandaloneJTAManagerLookup() throws Exception {
      doTest(new JBossStandaloneJTAManagerLookup());
   }
   
   protected void doTest(TransactionManagerLookup lookup) throws Exception {
      TransactionManager tm = lookup.getTransactionManager();
      tm.begin();
      tm.commit();

      tm.begin();
      tm.rollback();
   }

}
