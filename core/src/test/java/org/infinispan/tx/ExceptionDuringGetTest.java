package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus &lt;mircea.markus@jboss.com&gt; (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.ExceptionDuringGetTest")
public class ExceptionDuringGetTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(dcc, 2);
      waitForClusterToForm();
      cache(0).put("k", "v");
      assertEquals("v", cache(1).get("k"));
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "Induced!")
   public void testExceptionDuringGet() {
      extractInterceptorChain(advancedCache(0)).addInterceptorAfter(new ExceptionInterceptor(), PessimisticLockingInterceptor.class);
      cache(0).get("k");
      fail();
   }

   static class ExceptionInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         throw new RuntimeException("Induced!");
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         throw new RuntimeException("Induced!");
      }
   }
}
