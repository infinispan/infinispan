package org.infinispan.api.tree;

import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.impl.NodeKey;
import org.infinispan.tree.impl.TreeCacheImpl;
import org.infinispan.tree.impl.TreeStructureSupport;
import org.infinispan.commons.util.Util;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.*;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static org.testng.AssertJUnit.*;

/**
 * Exercises and tests the new move() api.
 *
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani</a>
 */
@Test(groups = "functional", testName = "api.tree.BaseNodeMoveAPITest")
public abstract class BaseNodeMoveAPITest extends SingleCacheManagerTest {
   protected final Log log = LogFactory.getLog(getClass());

   protected static final Fqn A = Fqn.fromString("/a"), B = Fqn.fromString("/b"), C = Fqn.fromString("/c"), D = Fqn.fromString("/d"), E = Fqn.fromString("/e");
   static final Fqn A_B = Fqn.fromRelativeFqn(A, B);
   static final Fqn A_B_C = Fqn.fromRelativeFqn(A_B, C);
   static final Fqn A_B_C_E = Fqn.fromRelativeFqn(A_B_C, E);
   static final Fqn C_E = Fqn.fromRelativeFqn(C, E);
   static final Fqn D_B = Fqn.fromRelativeFqn(D, B);
   static final Fqn D_B_C = Fqn.fromRelativeFqn(D_B, C);
   protected static final Object k = "key", vA = "valueA", vB = "valueB", vC = "valueC", vD = "valueD", vE = "valueE";

   TreeCacheImpl<Object, Object> treeCache;
   TransactionManager tm;
   DataContainer dc;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager();
      cm.defineConfiguration("test", createConfigurationBuilder().build());
      cache = cm.getCache("test");
      tm = TestingUtil.extractComponent(cache, TransactionManager.class);
      treeCache = new TreeCacheImpl<Object, Object>(cache);
      dc = TestingUtil.extractComponent(cache, DataContainer.class);
      return cm;
   }

   /**
    * Various subclasses will provide a suitable cache configuration builder.
    */
   protected abstract ConfigurationBuilder createConfigurationBuilder();

   public void testBasicMove() {
      Node<Object, Object> rootNode = treeCache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.put(k, vA);
      Node<Object, Object> nodeB = rootNode.addChild(B);
      nodeB.put(k, vB);
      Node<Object, Object> nodeC = nodeA.addChild(C);
      nodeC.put(k, vC);
      /*
        /a/c
        /b
      */

      assertTrue(rootNode.hasChild(A));
      assertTrue(rootNode.hasChild(B));
      assertFalse(rootNode.hasChild(C));
      assertTrue(nodeA.hasChild(C));

      // test data
      assertEquals("" + nodeA, vA, nodeA.get(k));
      assertEquals(vB, nodeB.get(k));
      assertEquals(vC, nodeC.get(k));

      // parentage
      assertEquals(nodeA, nodeC.getParent());

      log.info("BEFORE MOVE " + treeCache);
      // move
      treeCache.move(nodeC.getFqn(), nodeB.getFqn());

      // re-fetch nodeC
      nodeC = treeCache.getNode(Fqn.fromRelativeFqn(nodeB.getFqn(), C));

      log.info("POST MOVE " + treeCache);
      log.info("HC " + nodeC + " " + Util.hexIdHashCode(nodeC));
      Node x = treeCache.getRoot().getChild(Fqn.fromString("b/c"));
      log.info("HC " + x + " " + Util.hexIdHashCode(x));
      /*
         /a
         /b/c
      */
      assertEquals("NODE C " + nodeC, "/b/c", nodeC.getFqn().toString());

      assertTrue(rootNode.hasChild(A));
      assertTrue(rootNode.hasChild(B));
      assertFalse(rootNode.hasChild(C));
      assertFalse(nodeA.hasChild(C));
      assertTrue(nodeB.hasChild(C));

      // test data
      assertEquals(vA, nodeA.get(k));
      assertEquals(vB, nodeB.get(k));
      assertEquals(vC, nodeC.get(k));

      // parentage
      assertEquals("B is parent of C: " + nodeB, nodeB, nodeC.getParent());
   }

   @SuppressWarnings("unchecked")
   private Node<Object, Object> genericize(Node node) {
      return (Node<Object, Object>) node;
   }

   public void testMoveWithChildren() {
      Node<Object, Object> rootNode = treeCache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.put(k, vA);
      Node<Object, Object> nodeB = rootNode.addChild(B);
      nodeB.put(k, vB);
      Node<Object, Object> nodeC = nodeA.addChild(C);
      nodeC.put(k, vC);
      Node<Object, Object> nodeD = nodeC.addChild(D);
      nodeD.put(k, vD);
      Node<Object, Object> nodeE = nodeD.addChild(E);
      nodeE.put(k, vE);

      assertTrue(rootNode.hasChild(A));
      assertTrue(rootNode.hasChild(B));
      assertFalse(rootNode.hasChild(C));
      assertTrue(nodeA.hasChild(C));
      assertTrue(nodeC.hasChild(D));
      assertTrue(nodeD.hasChild(E));

      // test data
      assertEquals(vA, nodeA.get(k));
      assertEquals(vB, nodeB.get(k));
      assertEquals(vC, nodeC.get(k));
      assertEquals(vD, nodeD.get(k));
      assertEquals(vE, nodeE.get(k));

      // parentage
      assertEquals(rootNode, nodeA.getParent());
      assertEquals(rootNode, nodeB.getParent());
      assertEquals(nodeA, nodeC.getParent());
      assertEquals(nodeC, nodeD.getParent());
      assertEquals(nodeD, nodeE.getParent());

      // move
      log.info("move " + nodeC + " to " + nodeB);
      treeCache.move(nodeC.getFqn(), nodeB.getFqn());
      //log.debugf("nodeB " + nodeB);
      //log.debugf("nodeC " + nodeC);

      // child nodes will need refreshing, since existing pointers will be stale.
      nodeC = nodeB.getChild(C);
      nodeD = nodeC.getChild(D);
      nodeE = nodeD.getChild(E);

      assertTrue(rootNode.hasChild(A));
      assertTrue(rootNode.hasChild(B));
      assertFalse(rootNode.hasChild(C));
      assertFalse(nodeA.hasChild(C));
      assertTrue(nodeB.hasChild(C));
      assertTrue(nodeC.hasChild(D));
      assertTrue(nodeD.hasChild(E));

      // test data
      assertEquals(vA, nodeA.get(k));
      assertEquals(vB, nodeB.get(k));
      assertEquals(vC, nodeC.get(k));
      assertEquals(vD, nodeD.get(k));
      assertEquals(vE, nodeE.get(k));

      // parentage
      assertEquals(rootNode, nodeA.getParent());
      assertEquals(rootNode, nodeB.getParent());
      assertEquals(nodeB, nodeC.getParent());
      assertEquals(nodeC, nodeD.getParent());
      assertEquals(nodeD, nodeE.getParent());
   }

   public void testTxCommit() throws Exception {
      Node<Object, Object> rootNode = treeCache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);

      assertEquals(rootNode, nodeA.getParent());
      assertEquals(nodeA, nodeB.getParent());
      assertEquals(nodeA, rootNode.getChildren().iterator().next());
      assertEquals(nodeB, nodeA.getChildren().iterator().next());

      tm.begin();
      log.debugf("Before: " + TreeStructureSupport.printTree(treeCache, true));
      // move node B up to hang off the root
      treeCache.move(nodeB.getFqn(), Fqn.ROOT);
      log.debugf("After: " + TreeStructureSupport.printTree(treeCache, true));
      tm.commit();
      log.debugf("Committed: " + TreeStructureSupport.printTree(treeCache, true));
      nodeB = rootNode.getChild(B);

      assertEquals(rootNode, nodeA.getParent());
      assertEquals(rootNode, nodeB.getParent());

      assertTrue(rootNode.getChildren().contains(nodeA));
      assertTrue(rootNode.getChildren().contains(nodeB));

      assertTrue(nodeA.getChildren().isEmpty());
   }

   public void testTxRollback() throws Exception {
      Node<Object, Object> rootNode = treeCache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);

      assertEquals(rootNode, nodeA.getParent());
      assertEquals(nodeA, nodeB.getParent());
      assertEquals(nodeA, rootNode.getChildren().iterator().next());
      assertEquals(nodeB, nodeA.getChildren().iterator().next());


      tm.begin();
      // move node B up to hang off the root
      log.debugf("Before: " + TreeStructureSupport.printTree(treeCache, true));
      treeCache.move(nodeB.getFqn(), Fqn.ROOT);
      log.debugf("After: " + TreeStructureSupport.printTree(treeCache, true));
      tm.rollback();
      log.debugf("Rolled back: " + TreeStructureSupport.printTree(treeCache, true));

      nodeA = rootNode.getChild(A);
      nodeB = nodeA.getChild(B);

      // should revert
      assertEquals(rootNode, nodeA.getParent());
      assertEquals(nodeA, nodeB.getParent());
      assertEquals(nodeA, rootNode.getChildren().iterator().next());
      assertEquals(nodeB, nodeA.getChildren().iterator().next());
   }

   public void testLocksDeepMove() throws Exception {
      Node<Object, Object> rootNode = treeCache.getRoot();

      // Initial setup: /a/b/d, /c/e
      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);
      Node<Object, Object> nodeD = nodeB.addChild(D);
      Node<Object, Object> nodeC = rootNode.addChild(C);
      Node<Object, Object> nodeE = nodeC.addChild(E);
      assertNoLocks();
      tm.begin();

      // Move /c -> /a/b, new setup: /a/b/d, /a/b/c/e
      treeCache.move(nodeC.getFqn(), nodeB.getFqn());

      // /a/b, /c, /c/e, /a/b/c and /a/b/c/e should all be locked.
      assertTrue(isNodeLocked(C, true));
      assertTrue(isNodeLocked(C_E, true));
      assertTrue(isNodeLocked(A_B, false));
      assertTrue(isNodeLocked(A_B_C, true));
      assertTrue(isNodeLocked(A_B_C_E, true));

      tm.commit();

      assertNoLocks();
   }

   public void testLocks() throws Exception {
      Node<Object, Object> rootNode = treeCache.getRoot();

      // Initial setup: /a/b, /c
      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);
      Node<Object, Object> nodeC = rootNode.addChild(C);
      assertNoLocks();
      tm.begin();

      // Move /c -> /a/b/c
      treeCache.move(nodeC.getFqn(), nodeB.getFqn());

      assertTrue(isNodeLocked(C, true));
      assertTrue(isNodeLocked(A_B_C, true));
      assertTrue(isNodeLocked(Fqn.ROOT, false));
      assertTrue(isNodeLocked(A_B, false));

      tm.commit();
      assertNoLocks();
   }

   public void testConcurrentMoveSiblings() throws Exception {
      // tests a tree structure as such:
      // /a/x, a/y, /b, /c
      // N threads try to move /a/x from /a to /b and /a/y from /a to /c
      final int N = 5;
      final Fqn X = Fqn.fromString("/x"), Y = Fqn.fromString("/y");

      // set up the initial structure.
      Node<Object, Object> rootNode = treeCache.getRoot();
      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.addChild(X);
      nodeA.addChild(Y);
      Node<Object, Object> nodeB = rootNode.addChild(B);
      Node<Object, Object> nodeC = rootNode.addChild(C);

      Callable<Object>[] movers = new Callable[N];
      for (int i = 0; i < N; i++) {
         final Fqn source = Fqn.fromRelativeFqn(A, i % 2 == 0 ? X : Y);
         final Fqn dest = i % 2 == 0 ? B : C;
         movers[i] = new Callable<Object>() {
            public Object call() {
               try {
                  treeCache.move(source, dest);
               } catch (Exception e) {
                  // expected on some of the threads, in optimistic mode
               }
               return null;
            }
         };
      }

      runConcurrently(movers);

      log.info("Tree: " + TreeStructureSupport.printTree(treeCache, true));
      assertNoLocks();
      // Any of the move operations may fail, so just check that each node exists in exactly one place
      assertTrue(nodeA.getChildrenNames().contains("x") ^ nodeB.getChildrenNames().contains("x"));
      assertTrue(nodeA.getChildrenNames().contains("y") ^ nodeC.getChildrenNames().contains("y"));
   }

   public void testConcurrentMoveToSameDest() throws Exception {
      // tests a tree structure as such:
      // /a/x, /b/y, /c
      // N threads try to move /a/x to /c and /b/y to /c at the same time
      final int N = 5;
      final Fqn X = Fqn.fromString("/x"), Y = Fqn.fromString("/y");

      // set up the initial structure.
      Node<Object, Object> rootNode = treeCache.getRoot();
      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.addChild(X);
      Node<Object, Object> nodeB = rootNode.addChild(B);
      nodeB.addChild(Y);
      Node<Object, Object> nodeC = rootNode.addChild(C);

      Callable<Object>[] movers = new Callable[N];
      for (int i = 0; i < N; i++) {
         final Fqn source = i % 2 == 0 ? Fqn.fromRelativeFqn(A, X) : Fqn.fromRelativeFqn(B, Y);
         movers[i] = new Callable<Object>() {
            public Object call() throws Exception {
               try {
                  treeCache.move(source, C);
               } catch (Exception e) {
                  // expected on some of the threads, in optimistic mode
               }
               return null;
            }
         };
      }
      runConcurrently(movers);

      log.info("Tree: " + TreeStructureSupport.printTree(treeCache, true));
      assertNoLocks();
      // Any of the move operations may fail, so just check that each node exists in exactly one place
      assertTrue(nodeA.getChildrenNames().contains("x") ^ nodeC.getChildrenNames().contains("x"));
      assertTrue(nodeB.getChildrenNames().contains("y") ^ nodeC.getChildrenNames().contains("y"));
   }

   public void testConcurrentMoveSameNode() throws Exception {
      // set up the initial structure
      // /a, /b, /c
      // one thread tries to move /c under /a, another tries to move /c under /b
      Node<Object, Object> rootNode = treeCache.getRoot();
      final Fqn FQN_A = A, FQN_B = B, FQN_C = C;
      Node nodeA = rootNode.addChild(FQN_A);
      Node nodeB = rootNode.addChild(FQN_B);
      Node nodeC = rootNode.addChild(FQN_C);

      final CountDownLatch nodeReadLatch = new CountDownLatch(1);
      final CountDownLatch nodeMovedLatch = new CountDownLatch(1);

      // tries to move C under B right after another thread already moved C under A
      Callable<Object> moveCtoB = new Callable<Object>() {
         public Object call() throws Exception {
            tm().begin();
            try {
               // ensure we already 'see' node C in this tx. this is what actually triggers the issue.
               assertEquals(asSet("a", "b", "c"), treeCache.getRoot().getChildrenNames());
               assertEquals(Collections.emptySet(), treeCache.getNode(FQN_C).getChildrenNames());
               nodeReadLatch.countDown();

               nodeMovedLatch.await();
               treeCache.move(FQN_C, FQN_B);
               tm().commit(); //this is expected to fail
               fail("Transaction should have failed");
            } catch (Exception e) {
               if (tm().getTransaction() != null) {
                  // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                  try {
                     tm().rollback();
                  } catch (SystemException e1) {
                     log.error("Failed to rollback", e1);
                  }
               }
            }
            return null;
         }
      };

      // moves C under A successfully
      Callable<Object> moveCtoA = new Callable<Object>() {
         public Object call() throws Exception {
            tm().begin();
            try {
               try {
                  nodeReadLatch.await();
                  treeCache.move(FQN_C, FQN_A);
                  tm().commit();
               } finally {
                  nodeMovedLatch.countDown();
               }
            } catch (Exception e) {
               if (tm().getTransaction() != null) {
                  // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                  try {
                     tm().rollback();
                  } catch (SystemException e1) {
                     log.error("Failed to rollback", e1);
                  }
               }
               throw e;
            }
            return null;
         }
      };

      runConcurrently(moveCtoB, moveCtoA);

      log.trace("Tree: " + TreeStructureSupport.printTree(treeCache, true));
      assertNoLocks();
      assertFalse(nodeC.isValid());
      assertEquals(asSet("a", "b"), rootNode.getChildrenNames());
      assertEquals(asSet("c"), nodeA.getChildrenNames());
      assertEquals(Collections.emptySet(), nodeB.getChildrenNames());
   }

   public void testMoveInSamePlace() {
      Node<Object, Object> rootNode = treeCache.getRoot();
      final Fqn FQN_X = Fqn.fromString("/x");
      // set up the initial structure.
      Node aNode = rootNode.addChild(A);
      Node xNode = aNode.addChild(FQN_X);
      assertEquals(aNode.getChildren().size(), 1);

      log.debugf("Before: " + TreeStructureSupport.printTree(treeCache, true));

      treeCache.move(xNode.getFqn(), aNode.getFqn());

      log.debugf("After: " + TreeStructureSupport.printTree(treeCache, true));
      assertNoLocks();
      assertEquals(aNode.getChildren().size(), 1);
   }

   /**
    * Looks up the CacheEntry stored in transactional context corresponding to this AtomicMap.  If this AtomicMap
    * has yet to be touched by the current transaction, this method will return a null.
    * @return
    */
   protected CacheEntry lookupEntryFromCurrentTransaction(TransactionTable transactionTable, TransactionManager transactionManager, Object key) {
      // Prior to 5.1, this used to happen by grabbing any InvocationContext in ThreadLocal.  Since ThreadLocals
      // can no longer be relied upon in 5.1, we need to grab the TransactionTable and check if an ongoing
      // transaction exists, peeking into transactional state instead.
      try {
         LocalTransaction localTransaction = transactionTable.getLocalTransaction(transactionManager.getTransaction());

         // The stored localTransaction could be null, if this is the first call in a transaction.  In which case
         // we know that there is no transactional state to refer to - i.e., no entries have been looked up as yet.
         return localTransaction == null ? null : localTransaction.lookupEntry(key);
      } catch (SystemException e) {
         return null;
      }
   }

   protected boolean isNodeLocked(Fqn fqn, boolean includeData) {
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      TransactionTable tt = cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
      CacheEntry structure = lookupEntryFromCurrentTransaction(tt, tm, new NodeKey(fqn, NodeKey.Type.STRUCTURE));
      CacheEntry data = lookupEntryFromCurrentTransaction(tt, tm, new NodeKey(fqn, NodeKey.Type.DATA));
      return structure != null && data != null && structure.isChanged() && (!includeData || data.isChanged());
   }

   protected void assertNoLocks() {
      ComponentRegistry cr = TestingUtil.extractComponentRegistry(cache);
      LockManager lm = cr.getComponent(LockManager.class);
      LockAssert.assertNoLocks(lm);
   }

   public void testNonexistentSource() {
      treeCache.put(A_B_C, "k", "v");
      assert "v".equals(treeCache.get(A_B_C, "k"));
      assert 1 == treeCache.getNode(A_B).getChildren().size();
      assert treeCache.getNode(A_B).getChildrenNames().contains(C.getLastElement());
      assert !treeCache.getNode(A_B).getChildrenNames().contains(D.getLastElement());

      treeCache.move(D, A_B);

      assert "v".equals(treeCache.get(A_B_C, "k"));
      assert 1 == treeCache.getNode(A_B).getChildren().size();
      assert treeCache.getNode(A_B).getChildrenNames().contains(C.getLastElement());
      assert !treeCache.getNode(A_B).getChildrenNames().contains(D.getLastElement());
   }

   public void testNonexistentTarget() {
      treeCache.put(A_B_C, "k", "v");
      assert "v".equals(treeCache.get(A_B_C, "k"));
      assert 1 == treeCache.getNode(A_B).getChildren().size();
      assert treeCache.getNode(A_B).getChildrenNames().contains(C.getLastElement());
      assert null == treeCache.getNode(D);

      log.debugf(TreeStructureSupport.printTree(treeCache, true));

      treeCache.move(A_B, D);

      log.debugf(TreeStructureSupport.printTree(treeCache, true));

      assert null == treeCache.getNode(A_B_C);
      assert null == treeCache.getNode(A_B);
      assert null != treeCache.getNode(D);
      assert null != treeCache.getNode(D_B);
      assert null != treeCache.getNode(D_B_C);
      assert "v".equals(treeCache.get(D_B_C, "k"));
   }
   
   private Set<Object> asSet(Object... names) {
      return Util.asSet(names);
   }
}
