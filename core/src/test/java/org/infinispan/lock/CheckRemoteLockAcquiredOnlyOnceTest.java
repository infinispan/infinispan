package org.infinispan.lock;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

/**
 * This test makes sure that once a remote lock has been acquired, this acquisition attempt won't happen again during
 * the same transaction.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.CheckRemoteLockAcquiredOnlyOnceTest")
public class CheckRemoteLockAcquiredOnlyOnceTest extends MultipleCacheManagersTest {

   protected ControlInterceptor controlInterceptor;
   protected Object key;
   protected CacheMode mode = CacheMode.REPL_SYNC;

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(mode, true);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 2);
      waitForClusterToForm();
      controlInterceptor = new ControlInterceptor();
      cache(0).getAdvancedCache().addInterceptor(controlInterceptor, 1);
      key = "k";
   }

   public void testLockThenLock() throws Exception {
      testLockThenOperation(new CacheOperation() {
         @Override
         public void execute() {
            advancedCache(1).lock(key);
         }
      });
   }

   public void testLockThenPut() throws Exception {
      testLockThenOperation(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).put(key, "v");
         }
      });
   }

   public void testLockThenRemove() throws Exception {
      testLockThenOperation(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).remove(key);
         }
      });
   }

   public void testLockThenReplace() throws Exception {
      testLockThenOperation(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).replace(key, "", "v");
         }
      });
   }

   public void testLockThenPutAll() throws Exception {
      testLockThenOperation(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).putAll(Collections.singletonMap(key, "value"));
         }
      });
   }

   public void testPutThenLock() throws Exception {
      testOperationThenLock(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).put(key, "v");
         }
      });
   }

   public void testRemoveThenLock() throws Exception {
      testOperationThenLock(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).remove(key);
         }
      });
   }

   public void testReplaceThenLock() throws Exception {
      testOperationThenLock(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).replace(key, "", "v");
         }
      });
   }

   public void testPutAllThenLock() throws Exception {
      testOperationThenLock(new CacheOperation() {
         @Override
         public void execute() {
            cache(1).putAll(Collections.singletonMap(key, "value"));
         }
      });
   }

   private void testLockThenOperation(CacheOperation o) throws Exception {
      assert controlInterceptor.remoteInvocations.get() == 0;

      tm(1).begin();
      advancedCache(1).lock(key);
      assert lockManager(0).isLocked(key);
      assertEquals(controlInterceptor.remoteInvocations.get(), 1);

      for (int i = 0; i < 100; i++) {
         o.execute();
      }

      assertEquals(controlInterceptor.remoteInvocations.get(), 1);
      tm(1).commit();
      assertEquals(controlInterceptor.remoteInvocations.get(), 1);

      controlInterceptor.remoteInvocations.set(0);
   }

   private void testOperationThenLock(CacheOperation o) throws Exception {
      assert controlInterceptor.remoteInvocations.get() == 0;

      tm(1).begin();
      for (int i = 0; i < 100; i++) {
         o.execute();
      }
      assert lockManager(0).isLocked(key);
      assertEquals(controlInterceptor.remoteInvocations.get(), 1);

      advancedCache(1).lock(key);

      assertEquals(controlInterceptor.remoteInvocations.get(), 1);
      tm(1).commit();
      assertEquals(controlInterceptor.remoteInvocations.get(), 1);

      controlInterceptor.remoteInvocations.set(0);;
   }

   public static class ControlInterceptor extends CommandInterceptor {

      final AtomicInteger remoteInvocations = new AtomicInteger(0);

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) remoteInvocations.incrementAndGet();
         return super.visitLockControlCommand(ctx, command);
      }
   }

   public interface CacheOperation {
      void execute();
   }
}
