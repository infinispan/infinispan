package org.infinispan.client.hotrod.retry;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.jgroups.SuspectedException;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test different server error situations and check how clients behave under
 * those circumstances. Also verify whether failover is happening accordingly.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ServerFailureRetryTest")
public class ServerFailureRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      return hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
   }

   public void testRetryWithInfinispanSuspectException() {
      retryExceptions(false);
   }

   public void testRetryWithJGroupsSuspectedException() {
      retryExceptions(true);
   }

   private void retryExceptions(boolean throwJGroupsException) {
      AdvancedCache<?, ?> nextCache = nextCacheToHit();
      ErrorInducingInterceptor interceptor = new ErrorInducingInterceptor(throwJGroupsException);
      nextCache.addInterceptor(interceptor, 1);
      try {
         remoteCache.put(1, "v1");
         assertTrue(interceptor.suspectExceptionThrown);
         assertEquals("v1", remoteCache.get(1));
      } finally {
         nextCache.removeInterceptor(ErrorInducingInterceptor.class);
      }
   }

   public void testRetryCacheStopped() {
      // Put data in any of the cluster's default cache
      remoteCache.put(1, "v1");
      assertEquals("v1", remoteCache.get(1));
      // Find out what next cluster cache to be hit and stop it
      Cache<?, ?> cache = nextCacheToHit();
      try {
         cache.stop();
         remoteCache.put(2, "v2");
         assertEquals("v2", remoteCache.get(2));
      } finally {
         cache.start();
      }
   }

   // Listener callbacks can happen in the main thread or remote thread
   // depending on primary owner considerations, even for replicated caches.
   // Using an interceptor gives more flexibility by being able to put it
   // at the top of the interceptor stack, before remote/local separation
   public static class ErrorInducingInterceptor extends CommandInterceptor {
      volatile boolean suspectExceptionThrown;
      boolean throwJGroupsException;

      public ErrorInducingInterceptor(boolean throwJGroupsException) {
         this.throwJGroupsException = throwJGroupsException;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (ctx.isOriginLocal()) {
            suspectExceptionThrown = true;
            throw throwJGroupsException ?
                  new SuspectedException("Simulated suspicion")
                  : new SuspectException("Simulated suspicion");
         }
         return super.visitPutKeyValueCommand(ctx, command);
      }
   }

}
