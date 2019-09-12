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
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Tests if the {@link org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor} releases
 * locks if an exception occurs.
 *
 * @author Pedro Ruivo
 * @author Dan Berindei
 * @since 8.1
 */
@Test(groups = "functional", testName = "lock.NonTxRemoteLockTest")
public class NonTxRemoteLockTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      builder.clustering().stateTransfer().fetchInMemoryState(false);
      createClusteredCaches(2, TestDataSCI.INSTANCE, builder);
   }

   public void testExceptionBeforeLockingInterceptor() {
      final Object key = new MagicKey(cache(1));
      final LockManager lockManager = TestingUtil.extractLockManager(cache(1));
      TestingUtil.extractInterceptorChain(cache(1)).addInterceptorAfter(new ExceptionInRemotePutInterceptor(lockManager), NonTransactionalLockingInterceptor.class);
      assertFalse(lockManager.isLocked(key));

      try {
         cache(0).put(key, "foo");
         fail("Exception expected!");
      } catch (Exception e) {
         //expected
         assertEquals("Induced Exception!", e.getCause().getMessage());
      }

      //it sends the reply before invoke the finally. So, we need to use eventually :)
      eventually(() -> !lockManager.isLocked(key));
   }

   public static class ExceptionInRemotePutInterceptor extends DDAsyncInterceptor {
      LockManager lockManager;

      ExceptionInRemotePutInterceptor(LockManager lockManager) {
         this.lockManager = lockManager;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (ctx.isOriginLocal()) {
            return invokeNext(ctx, command);
         }
         assertTrue(lockManager.isLocked(command.getKey()));
         throw new RuntimeException("Induced Exception!");
      }
   }
}
