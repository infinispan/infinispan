/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.api.tree;

import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.NodeKey;
import org.infinispan.tree.TreeCacheImpl;
import org.infinispan.tree.TreeStructureSupport;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.testng.AssertJUnit.*;

/**
 * Excercises and tests the new move() api
 *
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani</a>
 */
@Test(groups = "functional", testName = "api.tree.NodeMoveAPITest")
public class NodeMoveAPITest extends SingleCacheManagerTest {
   protected final Log log = LogFactory.getLog(getClass());

   protected static final Fqn A = Fqn.fromString("/a"), B = Fqn.fromString("/b"), C = Fqn.fromString("/c"), D = Fqn.fromString("/d"), E = Fqn.fromString("/e");
   static final Fqn A_B = Fqn.fromRelativeFqn(A, B);
   static final Fqn A_B_C = Fqn.fromRelativeFqn(A_B, C);
   static final Fqn A_B_C_E = Fqn.fromRelativeFqn(A_B_C, E);
   static final Fqn A_B_D = Fqn.fromRelativeFqn(A_B, D);
   static final Fqn C_E = Fqn.fromRelativeFqn(C, E);
   static final Fqn D_B = Fqn.fromRelativeFqn(D, B);
   static final Fqn D_B_C = Fqn.fromRelativeFqn(D_B, C);
   protected static final Object k = "key", vA = "valueA", vB = "valueB", vC = "valueC", vD = "valueD", vE = "valueE";

   TreeCacheImpl<Object, Object> treeCache;
   TransactionManager tm;
   DataContainer dc;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      Configuration c = new Configuration().fluent()
            .stateRetrieval().fetchInMemoryState(false)
            .invocationBatching()
            .locking().lockAcquisitionTimeout(1000L)
            .build();
      cm.defineConfiguration("test", c);
      cache = cm.getCache("test");
      tm = TestingUtil.extractComponent(cache, TransactionManager.class);
      treeCache = new TreeCacheImpl(cache);
      dc = TestingUtil.extractComponent(cache, DataContainer.class);
      return cm;
   }

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
      //System.out.println("nodeB " + nodeB);
      //System.out.println("nodeC " + nodeC);

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
      System.out.println("Before: " + TreeStructureSupport.printTree(treeCache, true));
      // move node B up to hang off the root
      treeCache.move(nodeB.getFqn(), Fqn.ROOT);
      System.out.println("After: " + TreeStructureSupport.printTree(treeCache, true));
      tm.commit();
      System.out.println("Committed: " + TreeStructureSupport.printTree(treeCache, true));
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
      System.out.println("Before: " + TreeStructureSupport.printTree(treeCache, true));
      treeCache.move(nodeB.getFqn(), Fqn.ROOT);
      System.out.println("After: " + TreeStructureSupport.printTree(treeCache, true));
      tm.rollback();
      System.out.println("Rolled back: " + TreeStructureSupport.printTree(treeCache, true));

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

      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);
      Node<Object, Object> nodeD = nodeB.addChild(D);
      Node<Object, Object> nodeC = rootNode.addChild(C);
      Node<Object, Object> nodeE = nodeC.addChild(E);
      assertNoLocks();
      tm.begin();

      treeCache.move(nodeC.getFqn(), nodeB.getFqn());

      checkLocksDeep();


      tm.commit();

      assertNoLocks();
   }

   public void testLocks() throws Exception {
      Node<Object, Object> rootNode = treeCache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);
      Node<Object, Object> nodeC = rootNode.addChild(C);
      assertNoLocks();
      tm.begin();

      treeCache.move(nodeC.getFqn(), nodeB.getFqn());

      checkLocks();

      tm.commit();
      assertNoLocks();
   }

   public void testConcurrency() throws InterruptedException {
      Node<Object, Object> rootNode = treeCache.getRoot();

      final int N = 3;// number of threads
      final int loops = 1 << 6;// number of loops
      // tests a tree structure as such:
      // /a
      // /b
      // /c
      // /d
      // /e
      // /x
      // /y

      // N threads constantly move /x and /y around to hang off either /a ~ /e randomly.

      final Fqn FQN_A = A, FQN_B = B, FQN_C = C, FQN_D = D, FQN_E = E, FQN_X = Fqn.fromString("/x"), FQN_Y = Fqn.fromString("/y");

      // set up the initial structure.
      final Node[] NODES = {
            rootNode.addChild(FQN_A), rootNode.addChild(FQN_B),
            rootNode.addChild(FQN_C), rootNode.addChild(FQN_D), rootNode.addChild(FQN_E)
      };

      final Node<Object, Object> NODE_X = genericize(NODES[0]).addChild(FQN_X);
      final Node<Object, Object> NODE_Y = genericize(NODES[1]).addChild(FQN_Y);

      Thread[] movers = new Thread[N];
      final CountDownLatch latch = new CountDownLatch(1);
      final Random rnd = new Random();

      for (int i = 0; i < N; i++) {
         movers[i] = new Thread("Mover-" + i) {
            public void run() {
               try {
                  latch.await();
               }
               catch (InterruptedException e) {
               }

               for (int counter = 0; counter < loops; counter++) {

                  treeCache.move(NODE_X.getFqn(), NODES[rnd.nextInt(NODES.length)].getFqn());
                  TestingUtil.sleepRandom(250);
                  treeCache.move(NODE_Y.getFqn(), NODES[rnd.nextInt(NODES.length)].getFqn());
                  TestingUtil.sleepRandom(250);
               }
            }
         };
         movers[i].start();
      }

      latch.countDown();

      for (Thread t : movers) {
         t.join();
      }

      assertNoLocks();
      boolean found_x = false, found_x_again = false;
      for (Node erased : NODES) {
         Node<Object, Object> n = genericize(erased);
         if (!found_x) {
            found_x = n.hasChild(FQN_X);
         } else {
            found_x_again = found_x_again || n.hasChild(FQN_X);
         }
      }
      boolean found_y = false, found_y_again = false;
      for (Node erased : NODES) {
         Node<Object, Object> n = genericize(erased);
         if (!found_y) {
            found_y = n.hasChild(FQN_Y);
         } else {
            found_y_again = found_y_again || n.hasChild(FQN_Y);
         }
      }

      log.fatal("Tree: " + TreeStructureSupport.printTree(treeCache, true));

      assertTrue("Should have found x", found_x);
      assertTrue("Should have found y", found_y);
      assertFalse("Should have only found x once", found_x_again);
      assertFalse("Should have only found y once", found_y_again);
   }

   public void testMoveInSamePlace() {
      Node<Object, Object> rootNode = treeCache.getRoot();
      final Fqn FQN_X = Fqn.fromString("/x");
      // set up the initial structure.
      Node aNode = rootNode.addChild(A);
      Node xNode = aNode.addChild(FQN_X);
      assertEquals(aNode.getChildren().size(), 1);

      System.out.println("Before: " + TreeStructureSupport.printTree(treeCache, true));

      treeCache.move(xNode.getFqn(), aNode.getFqn());

      System.out.println("After: " + TreeStructureSupport.printTree(treeCache, true));

      assertEquals(aNode.getChildren().size(), 1);

      assertNoLocks();
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

   protected boolean isNodeLocked(Fqn fqn) {
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      TransactionTable tt = cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
      CacheEntry structure = lookupEntryFromCurrentTransaction(tt, tm, new NodeKey(fqn, NodeKey.Type.STRUCTURE));
      CacheEntry data = lookupEntryFromCurrentTransaction(tt, tm, new NodeKey(fqn, NodeKey.Type.DATA));
      return structure != null && data != null && structure.isChanged() && data.isChanged();
   }

   protected void checkLocks() {
      assert isNodeLocked(C) : "Node " + C + " is not locked!";
      assert isNodeLocked(A_B_C) : "Node " + A_B_C + " is not locked!";
   }

   protected void checkLocksDeep() {
      // /a/b, /c, /c/e, /a/b/c and /a/b/c/e should all be locked.
      assert isNodeLocked(C);
      assert isNodeLocked(C_E);
      assert isNodeLocked(A_B_C);
      assert isNodeLocked(A_B_C_E);
   }

   protected void assertNoLocks() {
      ComponentRegistry cr = TestingUtil.extractComponentRegistry(cache);
      LockManager lm = cr.getComponent(LockManager.class);
      InvocationContextContainer icc = cr.getComponent(InvocationContextContainer.class);
      LockAssert.assertNoLocks(lm, icc);
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

      System.out.println(TreeStructureSupport.printTree(treeCache, true));

      treeCache.move(A_B, D);

      System.out.println(TreeStructureSupport.printTree(treeCache, true));

      assert null == treeCache.getNode(A_B_C);
      assert null == treeCache.getNode(A_B);
      assert null != treeCache.getNode(D);
      assert null != treeCache.getNode(D_B);
      assert null != treeCache.getNode(D_B_C);
      assert "v".equals(treeCache.get(D_B_C, "k"));
   }
}
