package org.infinispan.context;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SingleKeyNonTxInvocationContextTest
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "context.SingleKeyNonTxInvocationContextTest")
public class SingleKeyNonTxInvocationContextTest extends MultipleCacheManagersTest {

   private CheckInterceptor ci0;
   private CheckInterceptor ci1;

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      c.clustering().hash().numOwners(1);
      createCluster(c, 2);
      waitForClusterToForm();
      ci0 = new CheckInterceptor();
      advancedCache(0).addInterceptor(ci0, 1);
      ci1 = new CheckInterceptor();
      advancedCache(1).addInterceptor(ci1, 1);
   }


   public void testPut() {
      assert !ci0.putOkay.get() && !ci1.putOkay.get();

      cache(0).put(getKeyForCache(0), "v");
      assert ci0.putOkay.get() && !ci1.putOkay.get();

      cache(1).put(getKeyForCache(1), "v");
      assert ci0.putOkay.get() && ci1.putOkay.get();
   }

   public void testRemove() {
      assert !ci0.removeOkay.get() && !ci1.removeOkay.get();

      cache(0).remove(getKeyForCache(0));
      assert ci0.removeOkay.get() && !ci1.removeOkay.get();

      cache(1).remove(getKeyForCache(1));
      assert ci0.removeOkay.get() && ci1.removeOkay.get();
   }

   public void testGet() {
      assert !ci0.getOkay.get() && !ci1.getOkay.get();

      cache(0).get(getKeyForCache(0));
      assert ci0.getOkay.get() && !ci1.getOkay.get();

      cache(1).get(getKeyForCache(1));
      assert ci0.getOkay.get() && ci1.getOkay.get();
   }

   public void testReplace() {
      assert !ci0.replaceOkay.get() && !ci1.replaceOkay.get();

      cache(0).replace(getKeyForCache(0), "v");
      assert ci0.replaceOkay.get() && !ci1.replaceOkay.get();

      cache(1).replace(getKeyForCache(1), "v");
      assert ci0.replaceOkay.get() && ci1.replaceOkay.get();
   }


   static class CheckInterceptor extends CommandInterceptor {

      private AtomicBoolean putOkay = new AtomicBoolean();
      private AtomicBoolean removeOkay = new AtomicBoolean();
      private AtomicBoolean getOkay = new AtomicBoolean();
      private AtomicBoolean replaceOkay = new AtomicBoolean();


      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (isRightType(ctx)) putOkay.set(true);
         return super.visitPutKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (isRightType(ctx)) removeOkay.set(true);
         return super.visitRemoveCommand(ctx, command);
      }

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (isRightType(ctx)) getOkay.set(true);
         return super.visitGetKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (isRightType(ctx)) replaceOkay.set(true);
         return super.visitReplaceCommand(ctx, command);
      }

      private boolean isRightType(InvocationContext ctx) {
         return ctx instanceof SingleKeyNonTxInvocationContext;
      }
   }
}
