package org.infinispan.api.tree;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.impl.TreeCacheImpl;

import static org.testng.AssertJUnit.*;

import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

/**
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani (manik AT jboss DOT org)</a>
 */
@Test(groups = "functional", testName = "api.tree.SyncReplTxTest")
public class SyncReplTxTest extends MultipleCacheManagersTest {
   private TreeCache<Object, Object> cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cb.invocationBatching().enable();

      createClusteredCaches(2, "replSync", cb);

      Cache c1 = cache(0, "replSync");
      Cache c2 = cache(1, "replSync");

      cache1 = new TreeCacheImpl<Object, Object>(c1);
      cache2 = new TreeCacheImpl<Object, Object>(c2);
   }

   private TransactionManager beginTransaction(Cache cache) throws NotSupportedException, SystemException {
      TransactionManager mgr = cache.getAdvancedCache().getTransactionManager();
      mgr.begin();
      return mgr;
   }

   public void testBasicOperation() throws SystemException, NotSupportedException, HeuristicMixedException, HeuristicRollbackException, RollbackException {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      Fqn f = Fqn.fromString("/test/data");
      String k = "key", v = "value";

      assertNull("Should be null", cache1.getRoot().getChild(f));
      assertNull("Should be null", cache2.getRoot().getChild(f));

      Node<Object, Object> node = cache1.getRoot().addChild(f);

      assertNotNull("Should not be null", node);

      TransactionManager tm = beginTransaction(cache1.getCache());
      node.put(k, v);
      tm.commit();

      assertEquals(v, node.get(k));
      assertEquals(v, cache1.get(f, k));
      assertEquals("Should have replicated", v, cache2.get(f, k));
   }
}