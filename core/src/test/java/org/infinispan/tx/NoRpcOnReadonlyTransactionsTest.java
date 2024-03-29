package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.org
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.NoRpcOnReadonlyTransactionsTest")
@CleanupAfterMethod
public class NoRpcOnReadonlyTransactionsTest extends MultipleCacheManagersTest {

   private TxCheckInterceptor i0;
   private TxCheckInterceptor i2;
   private TxCheckInterceptor i1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config.clustering().hash().numOwners(1);
      createCluster(TestDataSCI.INSTANCE, config, 3);
      waitForClusterToForm();

      i0 = new TxCheckInterceptor();
      i1 = new TxCheckInterceptor();
      i2 = new TxCheckInterceptor();
      extractInterceptorChain(advancedCache(0)).addInterceptor(i0, 1);
      extractInterceptorChain(advancedCache(1)).addInterceptor(i1, 1);
      extractInterceptorChain(advancedCache(2)).addInterceptor(i2, 1);
   }

   public void testReadOnlyTxNoNetworkCallAtCommit() throws Exception {
      Object k0 = getKeyForCache(0);
      log.tracef("On address %s adding key %s", address(0), k0);
      cache(0).put(k0, "v");
      assertEquals(0, i0.remotePrepares);
      assertEquals(0, i0.remoteCommits);
      assertEquals(0, i1.remotePrepares);
      assertEquals(0, i1.remoteCommits);
      assertEquals(0, i2.remotePrepares);
      assertEquals(0, i2.remoteCommits);


      log.trace("Here is where the r-o tx happens.");
      tm(1).begin();
      assertEquals("v", cache(1).get(k0));
      tm(1).commit();
      assertEquals(0, i0.remotePrepares);
      assertEquals(0, i0.remoteCommits);
      assertEquals(0, i1.remotePrepares);
      assertEquals(0, i1.remoteCommits);
      assertEquals(0, i2.remotePrepares);
      assertEquals(0, i2.remoteCommits);

      tm(1).begin();
      cache(1).put(getKeyForCache(2), "v");
      assertEquals("v", cache(1).get(k0));
      tm(1).commit();
      assertEquals(0, i0.remotePrepares);
      assertEquals(0, i0.remoteCommits);
      assertEquals(0, i1.remotePrepares);
      assertEquals(0, i1.remoteCommits);
      assertEquals(1, i2.remotePrepares);
      assertEquals(1, i2.remoteCommits);
   }

   public void testReadOnlyTxNoNetworkCallMultipleCaches() throws Exception {
      defineConfigurationOnAllManagers("a", new ConfigurationBuilder().read(manager(0).getDefaultCacheConfiguration(), Combine.DEFAULT));
      cache(0, "a");
      cache(1, "a");
      cache(2, "a");
      waitForClusterToForm("a");
      cache(0, "a").put("k", "v");

      assertEquals(0, i0.remotePrepares);
      assertEquals(0, i0.remoteCommits);
      assertEquals(0, i1.remotePrepares);
      assertEquals(0, i1.remoteCommits);
      assertEquals(0, i2.remotePrepares);
      assertEquals(0, i2.remoteCommits);

      assertEquals("v", cache(0, "a").get("k"));
      assertEquals("v", cache(1, "a").get("k"));
      assertEquals("v", cache(2, "a").get("k"));
      Object k0 = getKeyForCache(0);
      cache(0).put(k0, "v0");
      Object k1 = getKeyForCache(1);
      cache(1).put(k1, "v0");
      Object k2 = getKeyForCache(2);
      cache(2).put(k2, "v0");


      tm(1).begin();
      assertEquals("v", cache(1, "a").put("k", "v2"));
      assertEquals("v0", cache(1).get(k0));
      assertEquals("v0", cache(1).get(k1));
      assertEquals("v0", cache(1).get(k2));
      tm(1).commit();

      assertEquals(0, i0.remotePrepares);
      assertEquals(0, i0.remoteCommits);
      assertEquals(0, i1.remotePrepares);
      assertEquals(0, i1.remoteCommits);
      assertEquals(0, i2.remotePrepares);
      assertEquals(0, i2.remoteCommits);

      assertEquals("v2", cache(0, "a").get("k"));
      assertEquals("v2", cache(1, "a").get("k"));
      assertEquals("v2", cache(2, "a").get("k"));

   }

   static class TxCheckInterceptor extends DDAsyncInterceptor {
      private volatile int remotePrepares;
      private volatile int remoteCommits;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) remotePrepares++;
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) remoteCommits++;
         return super.visitCommitCommand(ctx, command);
      }

      void reset() {
         remotePrepares = remoteCommits = 0;
      }
   }
}
