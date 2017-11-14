package org.infinispan.tx.exception;

import static org.testng.Assert.assertEquals;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.CustomInterceptorConfigTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.CustomInterceptorExceptionTest")
public class CustomInterceptorExceptionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager eCm =
            TestCacheManagerFactory.createCacheManager(getDefaultClusteredCacheConfig(CacheMode.LOCAL, true));
      // Custom interceptor must be after TxInterceptor
      eCm.getCache().getAdvancedCache().addInterceptor(new CustomInterceptorConfigTest.DummyInterceptor() {
         @Override
         public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            throw new IllegalStateException("Induce failure!");
         }
      }, 4);
      return eCm;
   }

   public void testFailure() throws Exception {
      TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      try {
         cache.put("k", "v");
         assert false;
      } catch (Exception e) {
         log.debug("Ignoring expected exception during put", e);
         assertEquals(transactionManager.getTransaction().getStatus(), Status.STATUS_MARKED_ROLLBACK);
      }
   }
}
