package org.infinispan.tx.exception;

import jakarta.transaction.Status;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.ExceptionInCommandTest")
public class ExceptionInCommandTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true), 2);
      waitForClusterToForm();
   }

   public void testPutThrowsLocalException() throws Exception {
      tm(0).begin();
      try {
         cache(0).computeIfAbsent("k", (k) -> {
            throw new RuntimeException();
         });
         assert false;
      } catch (RuntimeException e) {
         assert tx(0).getStatus() == Status.STATUS_MARKED_ROLLBACK;
      }
   }
}
