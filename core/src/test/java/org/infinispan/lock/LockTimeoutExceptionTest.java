package org.infinispan.lock;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationExceptionFunction;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.LockTimeoutException;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests if the  {@link LockTimeoutException} is properly thrown and marshalled.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@Test(groups = "functional", testName = "lock.LockTimeoutExceptionTest")
public class LockTimeoutExceptionTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, TestDataSCI.INSTANCE, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   public void testCommandInvocationId() throws InterruptedException {
      final String cacheName = "non-tx";
      defineNewConfig(cacheName, false);

      Cache<MagicKey, String> cache0 = cache(0, cacheName);
      Cache<MagicKey, String> cache1 = cache(1, cacheName);

      final MagicKey key = new MagicKey(cache1);

      CommandInvocationId lockOwner = CommandInvocationId.generateId(address(cache1));
      KeyAwareLockPromise promise = TestingUtil.extractLockManager(cache1).lock(key, lockOwner, 0, TimeUnit.MILLISECONDS);
      promise.lock(); //make sure it is locked

      Exceptions.expectException(RemoteException.class, LockTimeoutException.class, () -> cache0.getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, key.toString()));

      assertLockOwnerOnException(extractCustomInterceptor(cache0), lockOwner);
      assertLockOwnerOnException(extractCustomInterceptor(cache1), lockOwner);

      TestingUtil.extractLockManager(cache1).unlock(key, lockOwner);
   }

   public void testCommandInvocationIdWithTx() throws InterruptedException {
      final String cacheName = "tx-with-non-tx-lock-owner";
      defineNewConfig(cacheName, true);

      Cache<MagicKey, String> cache0 = cache(0, cacheName);
      Cache<MagicKey, String> cache1 = cache(1, cacheName);

      final MagicKey key = new MagicKey(cache1);

      CommandInvocationId lockOwner = CommandInvocationId.generateId(address(cache1));
      KeyAwareLockPromise promise = TestingUtil.extractLockManager(cache1).lock(key, lockOwner, 0, TimeUnit.MILLISECONDS);
      promise.lock(); //make sure it is locked

      Exceptions.expectException(CacheException.class, () -> cache0.getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, key.toString()));

      assertLockOwnerOnException(extractCustomInterceptor(cache0), lockOwner);
      assertLockOwnerOnException(extractCustomInterceptor(cache1), lockOwner);

      TestingUtil.extractLockManager(cache1).unlock(key, lockOwner);
   }

   public void testGlobalTransaction() throws InterruptedException {
      final String cacheName = "tx";
      defineNewConfig(cacheName, true);

      Cache<MagicKey, String> cache0 = cache(0, cacheName);
      Cache<MagicKey, String> cache1 = cache(1, cacheName);

      final MagicKey key = new MagicKey(cache1);

      GlobalTransaction lockOwner = new GlobalTransaction(address(cache1), false);
      KeyAwareLockPromise promise = TestingUtil.extractLockManager(cache1).lock(key, lockOwner, 0, TimeUnit.MILLISECONDS);
      promise.lock(); //make sure it is locked

      Exceptions.expectException(CacheException.class, () -> cache0.getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, key.toString()));

      assertLockOwnerOnException(extractCustomInterceptor(cache0), lockOwner);
      assertLockOwnerOnException(extractCustomInterceptor(cache1), lockOwner);

      TestingUtil.extractLockManager(cache1).unlock(key, lockOwner);
   }

   public void testCustomLockOwner() throws InterruptedException {
      final String cacheName = "tx-with-custom";
      defineNewConfig(cacheName, true);

      Cache<MagicKey, String> cache0 = cache(0, cacheName);
      Cache<MagicKey, String> cache1 = cache(1, cacheName);

      final MagicKey key = new MagicKey(cache1);

      String lockOwner = "custom-owner";
      KeyAwareLockPromise promise = TestingUtil.extractLockManager(cache1).lock(key, lockOwner, 0, TimeUnit.MILLISECONDS);
      promise.lock(); //make sure it is locked

      Exceptions.expectException(CacheException.class, () -> cache0.getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, key.toString()));

      assertLockOwnerOnException(extractCustomInterceptor(cache0), lockOwner);
      assertLockOwnerOnException(extractCustomInterceptor(cache1), lockOwner);

      TestingUtil.extractLockManager(cache1).unlock(key, lockOwner);
   }

   private static void assertLockOwnerOnException(CustomInterceptor interceptor, Object lockOwner) {
      AssertJUnit.assertNotNull(interceptor.caught);
      AssertJUnit.assertEquals(lockOwner, interceptor.caught.getLockOwner());
   }

   private static CustomInterceptor extractCustomInterceptor(Cache<?, ?> cache) {
      //noinspection deprecation
      return cache.getAdvancedCache().getAsyncInterceptorChain().findInterceptorExtending(CustomInterceptor.class);
   }

   private void defineNewConfig(String cacheName, boolean transactional) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional);
      //noinspection deprecation
      builder.customInterceptors().addInterceptor().after(InvocationContextInterceptor.class).interceptorClass(CustomInterceptor.class);
      defineConfigurationOnAllManagers(cacheName, builder);
   }

   public static class CustomInterceptor extends DDAsyncInterceptor implements InvocationExceptionFunction<VisitableCommand> {

      private volatile LockTimeoutException caught;

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         try {
            return makeStage(invokeNext(ctx, command)).andExceptionally(ctx, command, this);
         } catch (Throwable throwable) {
            return apply(null, null, throwable);
         }
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Throwable throwable) throws Throwable {
         LockTimeoutException.findLockTimeoutException(throwable).ifPresent(e -> caught = e);
         throw throwable;
      }
   }
}
