package org.infinispan.tx.locking;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * @author Martin Gencur
 */
@Test(groups = "functional", testName = "tx.locking.SizeDistTxReadCommittedTest")
public class SizeDistTxReadCommittedTest extends SizeDistTxRepeatableReadTest {

   public SizeDistTxReadCommittedTest() {
      isolation = IsolationLevel.READ_COMMITTED;
   }

   /**
    * Manifestation of inconsistent behaviour of READ_COMMITTED mode. More information
    * in https://issues.jboss.org/browse/ISPN-4910 and in comments below.
    */
   @Override
   public void testSizeWithPreviousRead() throws Exception {
      preloadCacheAndCheckSize();

      tm(0).begin();
      //make sure we read k1 in this transaction
      assertEquals("v1", cache(0).get(k1));
      final Transaction tx1 = tm(0).suspend();

      //another tx working on the same keys
      tm(0).begin();
      //remove the key that was previously read in another tx
      cache(0).remove(k1);
      cache(0).put(k0, "v2");
      tm(0).commit();

      tm(0).resume(tx1);

      //We read k1 earlier and in READ_COMMITTED mode, the size() method should return the most
      //up-to-date size. However, the current transaction does not see the parallel changes
      //and still returns incorrect size == 2. The expected size would be 1.
      //assertEquals(1, cache(0).size()); //This assertion would fail
      assertEquals(2, cache(0).size());
      assertEquals("v2", cache(0).get(k0));

      //The current transaction does not see changes made to k1 in the parallel transaction.
      //As a result, the get() operation returns k1 instead of expected null.
      //assertNull(cache(0).get(k1)); //This assertion would fail
      assertEquals("v1", cache(0).get(k1));
      tm(0).commit();

      assertNull(cache(1).get(k1));
   }
}
