package org.infinispan.tx.totalorder.writeskew;

import static org.testng.Assert.assertEquals;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.ReplWriteSkewTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus &lt;mircea.markus@jboss.com&gt; (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.writeskew.TotalOrderWriteSkewTest")
@CleanupAfterMethod
public class TotalOrderWriteSkewTest extends ReplWriteSkewTest {

   public void transactionCleanupWithWriteSkew() throws Exception {
      cache(0).put("k", "v");
      tm(0).begin();
      assertEquals("v", cache(0).get("k"));
      cache(0).put("k", "v2");
      Transaction suspend = tm(0).suspend();

      cache(0).put("k", "v3");
      tm(0).resume(suspend);
      try {
         tm(0).commit();
         assert false;
      } catch (Throwable e) {
         log.debug("Ignoring expected write skew check exception", e);
      }
      assertEquals("v3", cache(0).get("k"));
      assertEventuallyEquals(1, "k", "v3");
      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .useSynchronization(false)
            .recovery().disable();
   }
}
