package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
import org.testng.annotations.Test;

@Test(testName = "tx.TransactionManagerLookupTreeTest", groups = "unit")
public class TransactionManagerLookupTreeTest extends TransactionManagerLookupTest {

   @Override
   protected void doTest(TransactionManagerLookup tml) {
      EmbeddedCacheManager ecm = null;
      try {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.transaction().transactionManagerLookup(tml)
               .invocationBatching().enable();
         ecm = TestCacheManagerFactory.createCacheManager(cb);
         TreeCache<Object, Object> tc = new TreeCacheFactory().createTreeCache(ecm.<Object, Object>getCache());
         tc.put("/a/b/c", "k", "v");
         assert "v".equals(tc.get("/a/b/c", "k"));
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }
}
