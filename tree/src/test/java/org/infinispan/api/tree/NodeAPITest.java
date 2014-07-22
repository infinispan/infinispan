package org.infinispan.api.tree;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.impl.TreeCacheImpl;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.AssertJUnit.*;

/**
 * Tests {@link Node}-centric operations
 *
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani</a>
 */
@Test(groups = "functional", testName = "api.tree.NodeAPITest")
public class NodeAPITest extends SingleCacheManagerTest {
   static final Fqn A = Fqn.fromString("/a"), B = Fqn.fromString("/b"), C = Fqn.fromString("/c"), D = Fqn.fromString("/d");
   Fqn A_B = Fqn.fromRelativeFqn(A, B);
   Fqn A_C = Fqn.fromRelativeFqn(A, C);
   TransactionManager tm;
   TreeCache<Object, Object> cache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(true);
      cb.invocationBatching().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cb);
      cache = new TreeCacheImpl<Object, Object>(cm.getCache());
      tm = cache.getCache().getAdvancedCache().getTransactionManager();
      return cm;
   }

   /**
    * Test method behaves according to javadoc.
    */
   public void testGetParent() {
      Node<Object, Object> rootNode = cache.getRoot();
      assertEquals(rootNode,  rootNode.getParent());

      Node<Object, Object> nodeA = rootNode.addChild(A);
      assertEquals(rootNode, nodeA.getParent());
   }

   public void testAddingData() {
      Node<Object, Object> rootNode = cache.getRoot();
      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.put("key", "value");

      assertEquals("value", nodeA.get("key"));
   }

   public void testAddingDataPutMap() {
      cache.put(A_B, Collections.singletonMap("key", "value"));
      assertEquals("value", cache.get(A_B, "key"));
   }

   public void testAddingDataPutKey() {
      cache.put(A_B, "key", "value");
      assertEquals("value", cache.get(A_B, "key"));
   }

   public void testAddingDataTx() throws Exception {
      Node<Object, Object> rootNode = cache.getRoot();
      tm.begin();
      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.put("key", "value");

      assertEquals("value", nodeA.get("key"));
      tm.commit();
   }

   public void testOverwritingDataTx() throws Exception {
      Node<Object, Object> rootNode = cache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      nodeA.put("key", "value");
      assertEquals("value", nodeA.get("key"));
      tm.begin();
      rootNode.removeChild(A);
      cache.put(A, "k2", "v2");
      tm.commit();
      assertNull(nodeA.get("key"));
      assertEquals("v2", nodeA.get("k2"));
   }


   /**
    * Remember, Fqns are relative!!
    */
   public void testParentsAndChildren() {

      Node<Object, Object> rootNode = cache.getRoot();

      Node<Object, Object> nodeA = rootNode.addChild(A);
      Node<Object, Object> nodeB = nodeA.addChild(B);
      Node<Object, Object> nodeC = nodeA.addChild(C);
      Node<Object, Object> nodeD = rootNode.addChild(D);

      assertEquals(rootNode, nodeA.getParent());
      assertEquals(nodeA, nodeB.getParent());
      assertEquals(nodeA, nodeC.getParent());
      assertEquals(rootNode, nodeD.getParent());

      assertTrue(rootNode.hasChild(A));
      assertFalse(rootNode.hasChild(B));
      assertFalse(rootNode.hasChild(C));
      assertTrue(rootNode.hasChild(D));

      assertTrue(nodeA.hasChild(B));
      assertTrue(nodeA.hasChild(C));

      assertEquals(nodeA, rootNode.getChild(A));
      assertEquals(nodeD, rootNode.getChild(D));
      assertEquals(nodeB, nodeA.getChild(B));
      assertEquals(nodeC, nodeA.getChild(C));

      assertTrue(nodeA.getChildren().contains(nodeB));
      assertTrue(nodeA.getChildren().contains(nodeC));
      assertEquals(2, nodeA.getChildren().size());

      assertTrue(rootNode.getChildren().contains(nodeA));
      assertTrue(rootNode.getChildren().contains(nodeD));
      assertEquals(2, rootNode.getChildren().size());

      assertEquals(true, rootNode.removeChild(A));
      assertFalse(rootNode.getChildren().contains(nodeA));
      assertTrue(rootNode.getChildren().contains(nodeD));
      assertEquals(1, rootNode.getChildren().size());

      assertEquals("double remove", false, rootNode.removeChild(A));
      assertEquals("double remove", false, rootNode.removeChild(A.getLastElement()));
   }


   public void testImmutabilityOfData() {

      Node<Object, Object> rootNode = cache.getRoot();

      rootNode.put("key", "value");
      Map<Object, Object> m = rootNode.getData();
      try {
         m.put("x", "y");
         fail("Map should be immutable!!");
      }
      catch (Exception e) {
         // expected
      }

      try {
         rootNode.getKeys().add(new Object());
         fail("Key set should be immutable");
      }
      catch (Exception e) {
         // expected
      }
   }

   public void testDefensiveCopyOfData() {

      Node<Object, Object> rootNode = cache.getRoot();

      rootNode.put("key", "value");
      Map<Object, Object> data = rootNode.getData();
      Set<Object> keys = rootNode.getKeys();

      assert keys.size() == 1;
      assert keys.contains("key");

      assert data.size() == 1;
      assert data.containsKey("key");

      // now change stuff.

      rootNode.put("key2", "value2");

      // assert that the collections we initially got have not changed.
      assert keys.size() == 1;
      assert keys.contains("key");

      assert data.size() == 1;
      assert data.containsKey("key");
   }

   public void testDefensiveCopyOfChildren() {

      Node<Object, Object> rootNode = cache.getRoot();

      Fqn childFqn = Fqn.fromString("/child");
      rootNode.addChild(childFqn).put("k", "v");
      Set<Node<Object, Object>> children = rootNode.getChildren();
      Set<Object> childrenNames = rootNode.getChildrenNames();

      assert childrenNames.size() == 1;
      assert childrenNames.contains(childFqn.getLastElement());

      assert children.size() == 1;
      assert children.iterator().next().getFqn().equals(childFqn);

      // now change stuff.

      rootNode.addChild(Fqn.fromString("/child2"));

      // assert that the collections we initially got have not changed.
      assert childrenNames.size() == 1;
      assert childrenNames.contains(childFqn.getLastElement());

      assert children.size() == 1;
      assert children.iterator().next().getFqn().equals(childFqn);
   }


   public void testImmutabilityOfChildren() {

      Node<Object, Object> rootNode = cache.getRoot();

      rootNode.addChild(A);

      try {
         rootNode.getChildren().clear();
         fail("Collection of child nodes returned in getChildren() should be immutable");
      }
      catch (Exception e) {
         // expected
      }
   }

   public void testGetChildAPI() {

      Node<Object, Object> rootNode = cache.getRoot();

      // creates a Node<Object, Object> with fqn /a/b/c
      Node childA = rootNode.addChild(A);
      childA.addChild(B).addChild(C);

      rootNode.getChild(A).put("key", "value");
      rootNode.getChild(A).getChild(B).put("key", "value");
      rootNode.getChild(A).getChild(B).getChild(C).put("key", "value");

      assertEquals("value", rootNode.getChild(A).get("key"));
      assertEquals("value", rootNode.getChild(A).getChild(B).get("key"));
      assertEquals("value", rootNode.getChild(A).getChild(B).getChild(C).get("key"));

      assertNull(rootNode.getChild(Fqn.fromElements("nonexistent")));
   }

   public void testClearingData() {

      Node<Object, Object> rootNode = cache.getRoot();

      rootNode.put("k", "v");
      rootNode.put("k2", "v2");
      assertEquals(2, rootNode.getKeys().size());
      rootNode.clearData();
      assertEquals(0, rootNode.getKeys().size());
      assertTrue(rootNode.getData().isEmpty());
   }

   public void testClearingDataTx() throws Exception {

      Node<Object, Object> rootNode = cache.getRoot();

      tm.begin();
      rootNode.put("k", "v");
      rootNode.put("k2", "v2");
      assertEquals(2, rootNode.getKeys().size());
      rootNode.clearData();
      assertEquals(0, rootNode.getKeys().size());
      assertTrue(rootNode.getData().isEmpty());
      tm.commit();
      assertTrue(rootNode.getData().isEmpty());
   }

   public void testPutData() {

      Node<Object, Object> rootNode = cache.getRoot();

      assertTrue(rootNode.getData().isEmpty());

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put("k1", "v1");
      map.put("k2", "v2");

      rootNode.putAll(map);

      assertEquals(2, rootNode.getData().size());
      assertEquals("v1", rootNode.get("k1"));
      assertEquals("v2", rootNode.get("k2"));

      map.clear();
      map.put("k3", "v3");

      rootNode.putAll(map);
      assertEquals(3, rootNode.getData().size());
      assertEquals("v1", rootNode.get("k1"));
      assertEquals("v2", rootNode.get("k2"));
      assertEquals("v3", rootNode.get("k3"));

      map.clear();
      map.put("k4", "v4");
      map.put("k5", "v5");

      rootNode.replaceAll(map);
      assertEquals(2, rootNode.getData().size());
      assertEquals("v4", rootNode.get("k4"));
      assertEquals("v5", rootNode.get("k5"));
   }

   public void testGetChildrenNames() throws Exception {

      Node<Object, Object> rootNode = cache.getRoot();

      rootNode.addChild(A).put("k", "v");
      rootNode.addChild(B).put("k", "v");

      Set<Object> childrenNames = new HashSet<Object>();
      childrenNames.add(A.getLastElement());
      childrenNames.add(B.getLastElement());

      assertEquals(childrenNames, rootNode.getChildrenNames());

      // now delete a child, within a tx
      tm.begin();
      rootNode.removeChild(B);
      assertFalse(rootNode.hasChild(B));
      childrenNames.remove(B.getLastElement());
      assertEquals(childrenNames, rootNode.getChildrenNames());
      tm.commit();
      assertEquals(childrenNames, rootNode.getChildrenNames());
   }

   public void testDoubleRemovalOfData() throws Exception {
      assert tm.getTransaction() == null;
      cache.put("/foo/1/2/3", "item", 1);
      assert tm.getTransaction() == null;
      assert 1 == (Integer) cache.get("/foo/1/2/3", "item");
      tm.begin();
      assert 1 == (Integer) cache.get("/foo/1/2/3", "item");
      cache.removeNode("/foo/1");
      assertNull(cache.getNode("/foo/1"));
      assertNull(cache.get("/foo/1", "item"));
      cache.removeNode("/foo/1/2/3");
      log.tracef("Cache: %s", cache);
      assertNull(cache.get("/foo/1/2/3", "item"));
      assertNull(cache.get("/foo/1", "item"));
      tm.commit();
      assertFalse(cache.exists("/foo/1"));
      assertNull(cache.get("/foo/1/2/3", "item"));
      assertNull(cache.get("/foo/1", "item"));
   }

   public void testDoubleRemovalOfData2() throws Exception {
      cache.put("/foo/1/2", "item", 1);
      tm.begin();
      assertEquals(cache.get("/foo/1", "item"), null);
      cache.removeNode("/foo/1");
      assertNull(cache.get("/foo/1", "item"));
      cache.removeNode("/foo/1/2");
      assertNull(cache.get("/foo/1", "item"));
      tm.commit();
      assertFalse(cache.exists("/foo/1"));
      assertNull(cache.get("/foo/1/2", "item"));
      assertNull(cache.get("/foo/1", "item"));
   }
}
