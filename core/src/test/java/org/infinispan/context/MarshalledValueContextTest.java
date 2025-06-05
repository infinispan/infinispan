package org.infinispan.context;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Key;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * This is to test that contexts are properly constructed and cleaned up wven when using marshalled values and the
 * explicit lock() API.
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test (testName = "context.MarshalledValueContextTest", groups = "functional")
public class MarshalledValueContextTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      c.memory().storage(StorageType.HEAP).encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM)
         .transaction().lockingMode(LockingMode.PESSIMISTIC);
      return TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, c);
   }

   public void testContentsOfContext() throws Exception {
      Cache<Key, String> c = cacheManager.getCache();
      ContextExtractingInterceptor cex = new ContextExtractingInterceptor();
      assertTrue(extractInterceptorChain(c).addInterceptorAfter(cex, InvocationContextInterceptor.class));

      c.put(new Key("k"), "v");

      assertEquals("v", c.get(new Key("k")));

      TransactionManager tm = c.getAdvancedCache().getTransactionManager();
      tm.begin();
      c.getAdvancedCache().lock(new Key("k"));

      LockManager lockManager = TestingUtil.extractComponent(c, LockManager.class);

      assertTrue(cex.ctx instanceof LocalTxInvocationContext);

      assertEquals("Looked up key should not be in transactional invocation context " +
            "as we don't perform any changes", 0, cex.ctx.lookedUpEntriesCount());
      assertEquals("Only one lock should be held", 1, lockManager.getNumberOfLocksHeld());

      c.put(new Key("k"), "v2");

      assertEquals("Still should only be one entry in the context", 1, cex.ctx.lookedUpEntriesCount());
      assertEquals("Only one lock should be held", 1, lockManager.getNumberOfLocksHeld());

      tm.commit();

      assertEquals("No locks should be held anymore", 0, lockManager.getNumberOfLocksHeld());

      assertEquals("v2", c.get(new Key("k")));
   }

   static class ContextExtractingInterceptor extends BaseAsyncInterceptor {
      InvocationContext ctx;
      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         this.ctx = ctx;
         return invokeNext(ctx, command);
      }
   }
}
