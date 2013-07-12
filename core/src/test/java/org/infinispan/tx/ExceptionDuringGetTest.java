package org.infinispan.tx;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
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
      assert cache(1).get("k").equals("v");
   }

   @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Induced!")
   public void testExceptionDuringGet() {
      advancedCache(0).addInterceptorAfter(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
            throw new RuntimeException("Induced!");
         }
      }, PessimisticLockingInterceptor.class);
      cache(0).get("k");
      assert false;
   }
}
