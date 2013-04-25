package org.infinispan.context;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

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
      final Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, false);
      c.fluent().hash().numOwners(1);
      createCluster(c, 2);
      waitForClusterToForm();
      ci0 = new CheckInterceptor();
      advancedCache(0).addInterceptor(ci0, 1);
      ci1 = new CheckInterceptor();
      advancedCache(1).addInterceptor(ci1, 1);
   }
   
   
   public void testPut() {
      assert !ci0.putOkay && !ci1.putOkay;
      
      cache(0).put(getKeyForCache(0), "v");
      assert ci0.putOkay && !ci1.putOkay;

      cache(1).put(getKeyForCache(1), "v");
      assert ci0.putOkay && ci1.putOkay;
   }
   
   public void testRemove() {
      assert !ci0.removeOkay && !ci1.removeOkay;
      
      cache(0).remove(getKeyForCache(0));
      assert ci0.removeOkay && !ci1.removeOkay;

      cache(1).remove(getKeyForCache(1));
      assert ci0.removeOkay && ci1.removeOkay;      
   }
   
   public void testGet() {
      assert !ci0.getOkay && !ci1.getOkay;
      
      cache(0).get(getKeyForCache(0));
      assert ci0.getOkay && !ci1.getOkay;

      cache(1).get(getKeyForCache(1));
      assert ci0.getOkay && ci1.getOkay;      
   }
   
   public void testReplace() {
      assert !ci0.replaceOkay && !ci1.replaceOkay;
      
      cache(0).replace(getKeyForCache(0), "v");
      assert ci0.replaceOkay && !ci1.replaceOkay;

      cache(1).replace(getKeyForCache(1), "v");
      assert ci0.replaceOkay && ci1.replaceOkay;      
   }


   static class CheckInterceptor extends CommandInterceptor {

      private boolean putOkay;
      private boolean removeOkay;
      private boolean getOkay;
      private boolean replaceOkay;


      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (isRightType(ctx)) putOkay = true;
         return super.visitPutKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (isRightType(ctx)) removeOkay = true;
         return super.visitRemoveCommand(ctx, command);
      }

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (isRightType(ctx)) getOkay = true;
         return super.visitGetKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (isRightType(ctx)) replaceOkay = true;
         return super.visitReplaceCommand(ctx, command);
      }

      private boolean isRightType(InvocationContext ctx) {
         return ctx instanceof SingleKeyNonTxInvocationContext;
      }
   }
}
