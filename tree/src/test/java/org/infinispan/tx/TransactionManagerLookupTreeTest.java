package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
import org.testng.annotations.Test;

@Test(testName = "tx.TransactionManagerLookupTreeTest", groups = "unit")
public class TransactionManagerLookupTreeTest extends TransactionManagerLookupTest {

   @Override
   protected void doTest(TransactionManagerLookup tml) {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setTransactionManagerLookup(tml);
         c.setInvocationBatchingEnabled(true);
         ecm = new DefaultCacheManager(c);
         TreeCache<Object, Object> tc = new TreeCacheFactory().createTreeCache(ecm.<Object, Object>getCache());
         tc.put("/a/b/c", "k", "v");
         assert "v".equals(tc.get("/a/b/c", "k"));
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }
}
