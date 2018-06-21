package org.infinispan.context;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

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
      c.memory().storageType(StorageType.BINARY)
         .transaction().lockingMode(LockingMode.PESSIMISTIC);
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testContentsOfContext() throws Exception {
      Cache<Key, String> c = cacheManager.getCache();
      ContextExtractingInterceptor cex = new ContextExtractingInterceptor();
      assertTrue(c.getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(cex, InvocationContextInterceptor.class));

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

   private static class ContextExtractingInterceptor extends CommandInterceptor {
      InvocationContext ctx;
      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         this.ctx = ctx;
         return super.invokeNextInterceptor(ctx, command);
      }
   }

   private static class Key implements Serializable, ExternalPojo {
      String actualKey;

      private Key(String actualKey) {
         this.actualKey = actualKey;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Key key = (Key) o;

         if (actualKey != null ? !actualKey.equals(key.actualKey) : key.actualKey != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return actualKey != null ? actualKey.hashCode() : 0;
      }
   }
}
