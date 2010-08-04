package org.infinispan.context;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextContainerImpl;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;

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
      Configuration c = TestCacheManagerFactory.getDefaultConfiguration(true);
      c.setUseLazyDeserialization(true);
      return new DefaultCacheManager(c);
   }

   public void testContentsOfContext() throws Exception {
      Cache<Key, String> c = cacheManager.getCache();
      c.put(new Key("k"), "v");

      assert "v".equals(c.get(new Key("k")));

      TransactionManager tm = c.getAdvancedCache().getTransactionManager();
      tm.begin();
      c.getAdvancedCache().lock(new Key("k"));

      InvocationContextContainer icc = TestingUtil.extractComponent(c, InvocationContextContainer.class);
      InvocationContext ctx = icc.getInvocationContext();

      assert ctx instanceof LocalTxInvocationContext;

      assert ctx.getLookedUpEntries().size() == 1 : "Looked up key should now be in the transactional invocation context";

      c.put(new Key("k"), "v2");

      assert ctx.getLookedUpEntries().size() == 1 : "Still should only be one entry in the context";

      tm.commit();

      assert ctx.getLookedUpEntries().size() == 0 : "Context should be cleared of looked up keys";

      assert "v2".equals(c.get(new Key("k")));
   }

   private static class Key implements Serializable {
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

