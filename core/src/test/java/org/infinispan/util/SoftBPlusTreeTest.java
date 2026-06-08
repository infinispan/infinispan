package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "util.SoftBPlusTreeTest")
public class SoftBPlusTreeTest {

   private static final int MIN_NODE_SIZE = 15;
   private static final int MAX_NODE_SIZE = 60;

   private final ConcurrentHashMap<Integer, byte[]> keysByValue = new ConcurrentHashMap<>();

   private final SoftBPlusTree.KeyLoader<Integer> keyLoader = value -> keysByValue.get(value);

   @BeforeMethod
   public void clearKeyMap() {
      keysByValue.clear();
   }

   private void putTracked(SoftBPlusTree<Integer> tree, byte[] key, int value) throws IOException {
      keysByValue.put(value, key.clone());
      tree.put(key, value);
   }

   private static byte[] key(String s) {
      return s.getBytes(StandardCharsets.UTF_8);
   }

   static final SoftBPlusTree.ValueSerializer<Integer> INT_SERIALIZER = new SoftBPlusTree.ValueSerializer<>() {
      @Override
      public void write(Integer value, ByteBuffer buffer) {
         buffer.putInt(value);
      }

      @Override
      public Integer read(ByteBuffer buffer) {
         return buffer.getInt();
      }

      @Override
      public int serializedSize(Integer value) {
         return 4;
      }
   };

   static class InMemoryNodeStore implements SoftBPlusTree.NodeStore {
      private byte[] data = new byte[4096];
      int writeCount = 0;

      @Override
      public void write(ByteBuffer buf, long offset) {
         writeCount++;
         int length = buf.remaining();
         ensureCapacity(offset + length);
         buf.get(data, (int) offset, length);
      }

      @Override
      public ByteBuffer read(long offset, int length) {
         byte[] result = new byte[length];
         System.arraycopy(data, (int) offset, result, 0, length);
         return ByteBuffer.wrap(result);
      }

      @Override
      public void truncate(long size) {
      }

      private void ensureCapacity(long required) {
         if (required > data.length) {
            int newLen = Math.max((int) required, data.length * 2);
            byte[] newData = new byte[newLen];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
         }
      }

      InMemoryNodeStore snapshot() {
         InMemoryNodeStore copy = new InMemoryNodeStore();
         copy.data = this.data.clone();
         return copy;
      }
   }

   public void testPerNodeSoftening() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }

      BPlusTree.Node<Integer> rootNode = tree.getRoot();
      if (rootNode instanceof BPlusTree.InnerNode<Integer> root) {
         for (BPlusTree.Node<Integer> child : root.children) {
            assertTrue("Child should be SoftNode after puts",
                  child instanceof SoftBPlusTree.SoftNode);
         }
      }
   }

   public void testSerializeDeserializeNodeRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      BPlusTree.Node<Integer> root = tree.getRoot();
      assertTrue("Root should be InnerNode for large tree", root instanceof BPlusTree.InnerNode);

      ByteBuffer serialized = SoftBPlusTree.serializeNode(root, INT_SERIALIZER);
      BPlusTree.Node<Integer> deserialized = tree.deserializeNode(serialized);

      assertTrue("Deserialized root should be InnerNode", deserialized instanceof BPlusTree.InnerNode);
      BPlusTree.InnerNode<Integer> innerDeserialized = (BPlusTree.InnerNode<Integer>) deserialized;
      BPlusTree.InnerNode<Integer> innerOriginal = (BPlusTree.InnerNode<Integer>) root;
      assertEquals(innerOriginal.children.length, innerDeserialized.children.length);

      for (BPlusTree.Node<Integer> child : innerDeserialized.children) {
         assertTrue("Deserialized children should be SoftNodes", child instanceof SoftBPlusTree.SoftNode);
      }
   }

   public void testSoftenChildrenAndGet() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      for (BPlusTree.Node<Integer> child : root.children) {
         assertTrue("Child should be SoftNode after load", child instanceof SoftBPlusTree.SoftNode);
      }

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }
   }

   public void testSoftenChildrenAndPublish() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 100;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      List<Integer> values = tree.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
   }

   public void testSoftNodeReloadAfterGC() throws Exception {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      loaded.clearSoftReferences();

      for (int i = 0; i < count; i++) {
         assertEquals("Value for key-" + String.format("%05d", i) + " should reload from store",
               Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }
   }

   public void testRepeatedReclamationAndReload() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      for (int cycle = 0; cycle < 3; cycle++) {
         tree.clearSoftReferences();
         for (int i = 0; i < count; i++) {
            assertEquals("Cycle " + cycle + ": value mismatch",
                  Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
         }
      }

      tree.clearSoftReferences();
      putTracked(tree, key("key-00050"), 9999);
      tree.clearSoftReferences();
      assertEquals(Integer.valueOf(9999), tree.get(key("key-00050")));
      assertEquals(Integer.valueOf(0), tree.get(key("key-00000")));
   }

   public void testPublishAfterReclamation() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 100;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      tree.clearSoftReferences();

      List<Integer> values = tree.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), values.get(i));
      }
   }

   public void testPutAfterSoften() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      putTracked(tree, key("key-00050"), 9999);

      BPlusTree.Node<Integer> rootNode = tree.getRoot();
      if (rootNode instanceof BPlusTree.InnerNode<Integer> root) {
         for (BPlusTree.Node<Integer> child : root.children) {
            assertTrue("All children should be SoftNodes", child instanceof SoftBPlusTree.SoftNode);
         }
      }

      assertEquals(Integer.valueOf(9999), tree.get(key("key-00050")));
      for (int i = 0; i < count; i++) {
         if (i == 50) continue;
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testRemoveAfterSoften() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      assertEquals(Integer.valueOf(100), tree.remove(key("key-00100")));
      assertEquals(count - 1, tree.size());
      assertNull(tree.get(key("key-00100")));

      for (int i = 0; i < count; i++) {
         if (i == 100) continue;
         assertEquals(Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }

   public void testReSoftenAfterModification() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      putTracked(tree, key("key-00050"), 9999);

      BPlusTree.Node<Integer> rootNode = tree.getRoot();
      if (rootNode instanceof BPlusTree.InnerNode<Integer> root) {
         for (BPlusTree.Node<Integer> child : root.children) {
            assertTrue("All children should be SoftNodes after auto re-softening",
                  child instanceof SoftBPlusTree.SoftNode);
         }
      }

      assertEquals(Integer.valueOf(9999), tree.get(key("key-00050")));
   }

   public void testSoftenOnSmallTree() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      putTracked(tree, key("a"), 1);
      putTracked(tree, key("b"), 2);

      assertTrue("Root should still be LeafNode for small tree",
            tree.getRoot() instanceof BPlusTree.LeafNode);

      assertEquals(Integer.valueOf(1), tree.get(key("a")));
      assertEquals(Integer.valueOf(2), tree.get(key("b")));
   }

   public void testSaveLoadAndModify() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      for (BPlusTree.Node<Integer> child : root.children) {
         assertTrue("Child should be SoftNode", child instanceof SoftBPlusTree.SoftNode);
      }

      putTracked(loaded, key("key-00050"), 9999);
      root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      for (BPlusTree.Node<Integer> child : root.children) {
         assertTrue("Child should be SoftNode after modify", child instanceof SoftBPlusTree.SoftNode);
      }

      assertEquals(Integer.valueOf(9999), loaded.get(key("key-00050")));
      assertEquals(Integer.valueOf(0), loaded.get(key("key-00000")));
   }

   public void testMixedOperationsWithSoftening() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      TreeMap<String, Integer> reference = new TreeMap<>();
      java.util.Random rng = new java.util.Random(42L);
      int valueCounter = 10000;

      for (int op = 0; op < 1000; op++) {
         int action = rng.nextInt(4);
         String k = "key-" + rng.nextInt(100);
         switch (action) {
            case 0: // put
               int val = valueCounter++;
               reference.put(k, val);
               putTracked(tree, key(k), val);
               break;
            case 1: // get
               Integer refVal = reference.get(k);
               Integer treeVal = tree.get(key(k));
               assertEquals("get(" + k + ") mismatch at op " + op, refVal, treeVal);
               break;
            case 2: // remove
               reference.remove(k);
               tree.remove(key(k));
               break;
            case 3: // clear
               reference.clear();
               tree.clear();
               break;
         }
         assertEquals("size mismatch at op " + op, reference.size(), tree.size());
      }

      for (Map.Entry<String, Integer> entry : reference.entrySet()) {
         assertEquals(entry.getValue(), tree.get(key(entry.getKey())));
      }
   }

   public void testPublishWithTakeAndSoftNodes() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      List<Integer> values = tree.<Integer>publish((k, v) -> v).take(50).toList().blockingGet();

      assertEquals(50, values.size());
      assertEquals(Integer.valueOf(0), values.get(0));
      assertEquals(Integer.valueOf(49), values.get(49));
   }

   public void testHeavyRemoveWithSoftening() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(15, 60, store, INT_SERIALIZER, keyLoader);
      TreeMap<String, Integer> reference = new TreeMap<>();
      java.util.Random rng = new java.util.Random(12345L);
      int valueCounter = 10000;

      for (int op = 0; op < 3000; op++) {
         int action = rng.nextInt(10);
         String k = String.format("seg/%03d/entry", rng.nextInt(200));
         if (action < 4) {
            int val = valueCounter++;
            reference.put(k, val);
            putTracked(tree, key(k), val);
         } else if (action < 8) {
            reference.remove(k);
            tree.remove(key(k));
         } else {
            Integer refVal = reference.get(k);
            Integer treeVal = tree.get(key(k));
            assertEquals("get(" + k + ") mismatch at op " + op, refVal, treeVal);
         }
         assertEquals("size mismatch at op " + op, reference.size(), tree.size());
      }

      for (Map.Entry<String, Integer> entry : reference.entrySet()) {
         assertEquals(entry.getValue(), tree.get(key(entry.getKey())));
      }
   }

   // --- saveTree / loadTree round-trip tests ---

   public void testSaveLoadTreeRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      for (int i = 0; i < count; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }

      BPlusTree.Node<Integer> root = loaded.getRoot();
      assertTrue("Root should be InnerNode", root instanceof BPlusTree.InnerNode);
      BPlusTree.InnerNode<Integer> inner = (BPlusTree.InnerNode<Integer>) root;
      for (BPlusTree.Node<Integer> child : inner.children) {
         assertTrue("Child should be SoftNode after loadTree", child instanceof SoftBPlusTree.SoftNode);
      }
   }

   public void testSaveLoadTreeSmallLeafRoot() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      putTracked(tree, key("a"), 1);
      putTracked(tree, key("b"), 2);

      assertTrue("Root should be LeafNode for small tree", tree.getRoot() instanceof BPlusTree.LeafNode);

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      assertEquals(Integer.valueOf(1), loaded.get(key("a")));
      assertEquals(Integer.valueOf(2), loaded.get(key("b")));
   }

   public void testSaveLoadTreeEmpty() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      assertNull("Empty tree should return null space", rootSpace);

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      assertEquals(0, loaded.size());
      assertNull(loaded.get(key("anything")));

      putTracked(loaded, key("hello"), 42);
      assertEquals(1, loaded.size());
      assertEquals(Integer.valueOf(42), loaded.get(key("hello")));
   }

   public void testSaveLoadTreeThenMutate() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);

      putTracked(loaded, key("key-00050"), 9999);
      assertEquals(Integer.valueOf(9999), loaded.get(key("key-00050")));
      loaded.remove(key("key-00100"));
      assertNull(loaded.get(key("key-00100")));

      assertEquals(Integer.valueOf(0), loaded.get(key("key-00000")));
      assertEquals(Integer.valueOf(199), loaded.get(key("key-00199")));
   }

   // --- Per-node efficiency tests ---

   public void testWriteCountIsProportionalToHeight() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 1000;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      int totalWritesBuild = store.writeCount;

      store.writeCount = 0;
      putTracked(tree, key("key-00500"), 9999);
      int writesForPut = store.writeCount;

      store.writeCount = 0;
      tree.remove(key("key-00300"));
      int writesForRemove = store.writeCount;

      assertTrue("Single put (" + writesForPut + ") should be much less than full build (" + totalWritesBuild + ")",
            writesForPut < totalWritesBuild / 5);
      assertTrue("Single remove (" + writesForRemove + ") should be much less than full build (" + totalWritesBuild + ")",
            writesForRemove < totalWritesBuild / 5);
   }

   public void testSoftNodeGetInsertionPoint() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      SoftBPlusTree.SoftNode<Integer> softChild = (SoftBPlusTree.SoftNode<Integer>) root.children[0];

      int point = softChild.getInsertionPoint(key("key-00000"));
      assertTrue("Insertion point should be >= 0", point >= 0);
   }

   public void testSoftNodeRightmostKey() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      int lastIdx = root.children.length - 1;
      SoftBPlusTree.SoftNode<Integer> lastChild = (SoftBPlusTree.SoftNode<Integer>) root.children[lastIdx];

      byte[] rightmost = lastChild.rightmostKey();
      assertEquals("Rightmost key of last child should be the last key in the tree",
            "key-00199", new String(rightmost, StandardCharsets.UTF_8));
   }

   public void testSoftNodeSplit() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      BPlusTree.InnerNode<Integer> root = (BPlusTree.InnerNode<Integer>) loaded.getRoot();
      SoftBPlusTree.SoftNode<Integer> softChild = (SoftBPlusTree.SoftNode<Integer>) root.children[0];

      List<BPlusTree.Node<Integer>> splitResult = softChild.split(MAX_NODE_SIZE);
      assertTrue("Split should return at least one node", splitResult.size() >= 1);
   }

   public void testLeafSerializedSize() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      putTracked(tree, key("a"), 1);
      putTracked(tree, key("b"), 2);
      putTracked(tree, key("c"), 3);

      BPlusTree.Node<Integer> root = tree.getRoot();
      assertTrue("Root should be LeafNode for small tree", root instanceof BPlusTree.LeafNode);

      ByteBuffer serialized = SoftBPlusTree.serializeNode(root, INT_SERIALIZER);
      int expectedSize = BPlusTree.INNER_NODE_HEADER_SIZE + 3 * INT_SERIALIZER.serializedSize(1);
      assertEquals("Leaf serialized size should be header + values", expectedSize, serialized.remaining());
   }

   public void testFreeBlockReuse() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      for (int i = 0; i < 500; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      long sizeAfterInsert = tree.getStoreSize();

      for (int i = 0; i < 250; i++) {
         tree.remove(key(String.format("key-%05d", i)));
      }

      for (int i = 500; i < 750; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      long sizeAfterReinsert = tree.getStoreSize();

      long growth = sizeAfterReinsert - sizeAfterInsert;
      assertTrue("Free block reuse should limit growth. Initial: " + sizeAfterInsert +
            ", after reinsert: " + sizeAfterReinsert + ", growth: " + growth,
            growth < sizeAfterInsert);
   }

   // --- IndexNodeOutdatedException retry tests ---

   public void testGetRetriesOnOutdatedNode() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      AtomicInteger failsRemaining = new AtomicInteger(2);
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> {
               if (failsRemaining.getAndDecrement() > 0) return null;
               return keysByValue.get(value);
            });
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      assertEquals(Integer.valueOf(100), loaded.get(key("key-00100")));
      assertTrue("Should have retried at least twice", failsRemaining.get() < 0);
   }

   public void testGetExceedsMaxRetriesOnOutdatedNode() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> null);
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      try {
         loaded.get(key("key-00100"));
         throw new AssertionError("Should have thrown IndexNodeOutdatedException");
      } catch (SoftBPlusTree.IndexNodeOutdatedException e) {
         // expected after 11 failed attempts
      }
   }

   public void testPublishRetriesOnOutdatedFunction() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < 10; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      AtomicInteger attempts = new AtomicInteger(0);
      List<Integer> values = tree.<Integer>publish((k, v) -> {
         if (v == 0 && attempts.getAndIncrement() < 2) {
            throw new SoftBPlusTree.IndexNodeOutdatedException("transient");
         }
         return v;
      }).toList().blockingGet();

      assertEquals(10, values.size());
      assertEquals(Integer.valueOf(0), values.get(0));
      assertEquals(Integer.valueOf(9), values.get(9));
      assertTrue("Should have retried at least twice", attempts.get() >= 3);
   }

   public void testPublishExceedsMaxRetries() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < 10; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      try {
         tree.<Integer>publish((k, v) -> {
            throw new SoftBPlusTree.IndexNodeOutdatedException("always outdated");
         }).toList().blockingGet();
         throw new AssertionError("Should have thrown");
      } catch (RuntimeException e) {
         Throwable cause = e;
         while (cause != null && !(cause instanceof IllegalStateException)) {
            cause = cause.getCause();
         }
         assertTrue("Should contain IllegalStateException in cause chain, got: " + e,
               cause instanceof IllegalStateException);
         assertTrue("Message should mention retry attempts",
               cause.getMessage().contains("retry attempts"));
      }
   }

   public void testPublishSkipOnOutdated() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      for (int i = 0; i < 10; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      AtomicInteger outdatedCount = new AtomicInteger(0);
      List<Integer> values = tree.<Integer>publish((k, v) -> {
         if (v == 3 || v == 7) {
            outdatedCount.incrementAndGet();
            throw new SoftBPlusTree.IndexNodeOutdatedException("outdated");
         }
         return v;
      }, true).toList().blockingGet();

      assertEquals("Should skip outdated entries", 8, values.size());
      assertTrue("Should not contain skipped value 3", !values.contains(3));
      assertTrue("Should not contain skipped value 7", !values.contains(7));
      assertEquals("Each outdated entry should be encountered once", 2, outdatedCount.get());
   }

   public void testPublishSkipOnOutdatedRetriesNodeResolution() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      AtomicInteger failsRemaining = new AtomicInteger(2);
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> {
               if (failsRemaining.getAndDecrement() > 0) return null;
               return keysByValue.get(value);
            });
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      List<Integer> values = loaded.<Integer>publish((k, v) -> v, true).toList().blockingGet();
      assertEquals(count, values.size());
      assertTrue("Should have retried node resolution", failsRemaining.get() < 0);
   }

   public void testPublishRetriesOnOutdatedNode() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);
      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();

      AtomicInteger failsRemaining = new AtomicInteger(2);
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store.snapshot(), INT_SERIALIZER,
            value -> {
               if (failsRemaining.getAndDecrement() > 0) return null;
               return keysByValue.get(value);
            });
      loaded.loadTree(rootSpace);
      loaded.clearSoftReferences();

      List<Integer> values = loaded.<Integer>publish((k, v) -> v).toList().blockingGet();
      assertEquals(count, values.size());
      assertTrue("Should have retried", failsRemaining.get() < 0);
   }

   /**
    * With minNodeSize=0, tryInPlaceLeafUpdate can persist an empty leaf to disk
    * when the leaf is the leftmost child (index 0) — the leftmost-key-change check
    * is skipped, so the empty leaf bypasses applyModification entirely. Without the
    * guard in tryInPlaceLeafUpdate, subsequent puts that cause an ancestor to split
    * can produce a piece with only empty-leaf descendants, throwing
    * IllegalStateException from copyWith.
    */
   public void testEmptyLeafViaInPlaceUpdateWithMinNodeSizeZero() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(0, 60, store, INT_SERIALIZER, keyLoader);
      String prefix = "RBROOT/page";
      int maxPages = 30;
      int entitiesPerPage = 8;

      // Phase 1: Populate all pages
      for (int page = 0; page < maxPages; page++) {
         for (int e = 0; e < entitiesPerPage; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            putTracked(tree, key(k), page * 100 + e);
         }
      }

      // Phase 2: Remove all entries starting from page 0
      // When a leaf at index 0 (leftmost child) is emptied, tryInPlaceLeafUpdate
      // skips the leftmost-key-change check and persists the empty leaf to disk,
      // bypassing applyModification and manageLength entirely
      for (int page = 0; page < 20; page++) {
         for (int e = 0; e < entitiesPerPage; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            tree.remove(key(k));
         }
      }

      // Phase 3: Insert more entries to force ancestor splits
      for (int page = 0; page < 5; page++) {
         for (int e = entitiesPerPage; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            putTracked(tree, key(k), page * 100 + e);
         }
      }
      for (int page = 25; page < 30; page++) {
         for (int e = entitiesPerPage; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            putTracked(tree, key(k), page * 100 + e);
         }
      }

      // Verify all remaining entries are correct
      // Pages 0-4: entities 0-7 were removed in Phase 2, only 8-79 remain
      for (int page = 0; page < 5; page++) {
         for (int e = entitiesPerPage; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            assertEquals("get(" + k + ")", Integer.valueOf(page * 100 + e), tree.get(key(k)));
         }
      }
      for (int page = 25; page < 30; page++) {
         for (int e = 0; e < 80; e++) {
            String k = String.format("%s:%02d:entity:%04d", prefix, page, e);
            assertEquals("get(" + k + ")", Integer.valueOf(page * 100 + e), tree.get(key(k)));
         }
      }
   }

   public void testSerializeDeserializeFreeBlocksRoundTrip() throws IOException {
      InMemoryNodeStore store = new InMemoryNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, store, INT_SERIALIZER, keyLoader);

      for (int i = 0; i < 500; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }
      for (int i = 0; i < 250; i++) {
         tree.remove(key(String.format("key-%05d", i)));
      }

      SoftBPlusTree.NodeSpace rootSpace = tree.saveTree();
      long storeSizeBefore = tree.getStoreSize();
      ByteBuffer freeBlocksBuf = tree.serializeFreeBlocks();
      assertTrue("Free blocks buffer should contain data", freeBlocksBuf.remaining() > 4);

      InMemoryNodeStore loadStore = store.snapshot();
      SoftBPlusTree<Integer> loaded = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE, loadStore, INT_SERIALIZER, keyLoader);
      loaded.setStoreSize(storeSizeBefore);
      loaded.deserializeFreeBlocks(freeBlocksBuf);
      loaded.loadTree(rootSpace);

      for (int i = 250; i < 500; i++) {
         assertEquals(Integer.valueOf(i), loaded.get(key(String.format("key-%05d", i))));
      }

      long sizeBeforeReinsert = loaded.getStoreSize();
      for (int i = 0; i < 250; i++) {
         putTracked(loaded, key(String.format("key-%05d", i)), i + 1000);
      }
      long sizeAfterReinsert = loaded.getStoreSize();

      assertTrue("Restored free blocks should be reused, limiting growth. Before: " + sizeBeforeReinsert +
            ", after: " + sizeAfterReinsert,
            sizeAfterReinsert - sizeBeforeReinsert < sizeBeforeReinsert);
   }

   // --- Alignment padding coverage test ---

   /**
    * A NodeStore that tracks the high-water mark of writes and throws IOException
    * when reads go past the written extent, simulating FileChannel EOF behavior.
    * This is how {@link org.infinispan.persistence.sifs.Index.IndexFileNodeStore}
    * behaves: FileChannel.read() returns -1 when reading past the file's actual size.
    */
   static class FileSimulatingNodeStore implements SoftBPlusTree.NodeStore {
      private byte[] data = new byte[4096];
      private long fileSize = 0;

      @Override
      public void write(ByteBuffer buf, long offset) {
         int length = buf.remaining();
         ensureCapacity(offset + length);
         buf.get(data, (int) offset, length);
         long writeEnd = offset + length;
         if (writeEnd > fileSize) {
            fileSize = writeEnd;
         }
      }

      @Override
      public ByteBuffer read(long offset, int length) throws IOException {
         if (offset >= fileSize) {
            throw new IOException("Truncated node store at offset " + offset);
         }
         if (offset + length > fileSize) {
            throw new IOException("Truncated node store at offset " + offset
                  + " (requested " + length + " bytes, file size " + fileSize + ")");
         }
         byte[] result = new byte[length];
         System.arraycopy(data, (int) offset, result, 0, length);
         return ByteBuffer.wrap(result);
      }

      @Override
      public void truncate(long size) {
         fileSize = size;
      }

      private void ensureCapacity(long required) {
         if (required > data.length) {
            int newLen = Math.max((int) required, data.length * 2);
            byte[] newData = new byte[newLen];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
         }
      }
   }

   /**
    * Reproduces the "Truncated node store at offset" bug. When a node's serialized
    * size is not a multiple of the block alignment, writeNodeToStore() must write the
    * full aligned block (not just the serialized bytes) so the file extends to cover
    * the SoftNode's occupiedSpace. Without this fix, the last block written by
    * persistNewNodes() leaves a gap between serialized data and the aligned block end,
    * causing SoftNode.resolve() to hit EOF after GC clears the SoftReference.
    */
   public void testAlignmentPaddingWrittenToStore() throws IOException {
      short blockAlignment = 64;
      long headerSize = 26;
      FileSimulatingNodeStore store = new FileSimulatingNodeStore();
      SoftBPlusTree<Integer> tree = new SoftBPlusTree<>(MIN_NODE_SIZE, MAX_NODE_SIZE,
            store, INT_SERIALIZER, keyLoader, blockAlignment, headerSize);

      int count = 200;
      for (int i = 0; i < count; i++) {
         putTracked(tree, key(String.format("key-%05d", i)), i);
      }

      tree.clearSoftReferences();

      for (int i = 0; i < count; i++) {
         assertEquals("Value for key-" + String.format("%05d", i) + " should resolve from disk",
               Integer.valueOf(i), tree.get(key(String.format("key-%05d", i))));
      }
   }
}
