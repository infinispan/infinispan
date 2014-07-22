package org.infinispan.tx.exception;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.CustomInterceptorConfigTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.InvocationContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.CustomInterceptorException")
public class CustomInterceptorException extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager eCm =
            TestCacheManagerFactory.createCacheManager(getDefaultClusteredCacheConfig(CacheMode.LOCAL, true));
      eCm.getCache().getAdvancedCache().addInterceptor(new CustomInterceptorConfigTest.DummyInterceptor() {
         @Override
         public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            throw new IllegalStateException("Induce failure!");
         }
      }, 1);
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
