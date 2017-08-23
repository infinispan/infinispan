package org.infinispan.lock;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Tests if the {@link org.infinispan.remoting.inboundhandler.NonTotalOrderPerCacheInboundInvocationHandler} releases
 * locks if an exception occurs before the locking interceptor.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
@Test(groups = "functional", testName = "lock.SimpleRemoteLockTest")
public class SimpleRemoteLockTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      builder.clustering().stateTransfer().fetchInMemoryState(false);
      builder.customInterceptors().addInterceptor().before(NonTransactionalLockingInterceptor.class).interceptorClass(ExceptionInRemotePutInterceptor.class);
      createClusteredCaches(2, builder);
   }

   public void testExceptionBeforeLockingInterceptor() {
      final Object key = new MagicKey(cache(1));
      final LockManager lockManager = TestingUtil.extractLockManager(cache(1));
      assertFalse(lockManager.isLocked(key));

      try {
         cache(0).put(key, "foo");
         fail("Exception expected!");
      } catch (Exception e) {
         //expected
         assertEquals("Induced Exception!", e.getCause().getMessage());
      }

      //it sends the reply before invoke the finally. So, we need to use eventually :)
      Eventually.eventually(() -> !lockManager.isLocked(key));
   }

   public static class ExceptionInRemotePutInterceptor extends BaseCustomInterceptor {
      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (ctx.isOriginLocal()) {
            return invokeNextInterceptor(ctx, command);
         }
         assertTrue(TestingUtil.extractLockManager(cache).isLocked(command.getKey()));
         throw new RuntimeException("Induced Exception!");
      }
   }
}
