package org.infinispan.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Util;
import org.infinispan.reactive.FlowableCreate;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;

/**
 * An in-memory B+ tree with prefix-compressed {@code byte[]} keys and generic values.
 * <p>
 * Concurrent reads are safe against single-threaded writes: node fields are {@code final}
 * for safe publication and the root reference is {@code volatile}. Callers must ensure
 * at most one writer at a time. When a mutation creates new nodes that propagate up to
 * the root, the volatile write to {@code root} provides a happens-before edge and readers
 * see a fully consistent snapshot. However, two mutations are performed in-place without
 * updating the root: value overwrites (existing key gets a new value) and child replacements
 * when the new node fits within {@code maxNodeSize}. In these cases there is no volatile
 * write, so readers on other threads may not see the update immediately on weakly-ordered
 * architectures. This is acceptable for the SIFS index use case where the cache layer above
 * handles consistency. A stricter use case would require {@code VarHandle} release/acquire
 * semantics on the array elements, or always path-copying up to root.
 * <p>
 * Subclasses may extend {@link Node} to add custom behavior (e.g. soft-referenced
 * disk-backed nodes). Override {@link Node#resolve()} to transparently unwrap such
 * wrappers during traversal.
 *
 * @param <V> value type
 */
public class BPlusTree<V> {
   // Prefix length (short) + keyParts count (short) + flag (byte)
   static final int INNER_NODE_HEADER_SIZE = 5;
   static final int INNER_NODE_REFERENCE_SIZE = 10;
   static final int LEAF_NODE_REFERENCE_SIZE = 12;

   /**
    * The minimum node size that guarantees room for the header plus at least two child
    * references. Callers should ensure {@code maxNodeSize >= RESERVED_SPACE}.
    */
   public static final int RESERVED_SPACE
         = INNER_NODE_HEADER_SIZE + 2 * Math.max(INNER_NODE_REFERENCE_SIZE, LEAF_NODE_REFERENCE_SIZE);

   protected final int minNodeSize;
   protected final int maxNodeSize;

   private volatile Node<V> root;
   private volatile int size;
   private final AtomicInteger modCount = new AtomicInteger();

   /**
    * Creates an empty B+ tree.
    *
    * @param minNodeSize minimum serialized node size in bytes before a merge is attempted
    * @param maxNodeSize maximum serialized node size in bytes before a split is triggered
    */
   public BPlusTree(int minNodeSize, int maxNodeSize) {
      this.minNodeSize = minNodeSize;
      this.maxNodeSize = maxNodeSize;
      this.root = new LeafNode<>(Util.EMPTY_BYTE_ARRAY_ARRAY, emptyValues(), INNER_NODE_HEADER_SIZE);
   }

   @SuppressWarnings("unchecked")
   private V[] emptyValues() {
      return (V[]) new Object[0];
   }

   Node<V> getRoot() {
      return root;
   }

   /** Returns the number of entries in the tree. */
   public int size() {
      return size;
   }

   void setRoot(Node<V> root) {
      this.root = root;
   }

   void setSize(int size) {
      this.size = size;
   }

   protected void onChildrenReplaced(Node<V>[] oldChildren, int from, int to) {
   }

   protected void onValueOverwritten(Deque<PathEntry<V>> stack, LeafNode<V> leaf,
         int entryIndex) {
   }

   protected boolean tryInPlaceLeafUpdate(Deque<PathEntry<V>> stack, LeafNode<V> oldLeaf,
         LeafNode<V> newLeaf) {
      return false;
   }

   /**
    * Look up a key. Returns the value or null if the key is not present.
    */
   public V get(byte[] key) {
      Node<V> node = root;
      while (node instanceof InnerNode<V> inner) {
         int idx = inner.getInsertionPoint(key);
         node = inner.children[idx].resolve();
      }
      LeafNode<V> leaf = (LeafNode<V>) node;
      int idx = leaf.getInsertionPoint(key);
      if (idx >= 0) {
         return leaf.values[idx];
      }
      return null;
   }

   /**
    * Insert or replace a value. Returns the previous value, or null if the key was not present.
    */
   public V put(byte[] key, V value) {
      int mc = modCount.incrementAndGet();
      Deque<PathEntry<V>> stack = new ArrayDeque<>();
      Node<V> node = root;
      while (node instanceof InnerNode<V> inner) {
         int idx = inner.getInsertionPoint(key);
         stack.push(new PathEntry<>(inner, idx));
         node = inner.children[idx].resolve();
      }
      LeafNode<V> leaf = (LeafNode<V>) node;

      int insertPart = leaf.getInsertionPoint(key);

      if (insertPart >= 0) {
         V old = leaf.values[insertPart];
         leaf.values[insertPart] = value;
         onValueOverwritten(stack, leaf, insertPart);
         checkModCount(mc);
         return old;
      }
      insertPart = ~insertPart;

      LeafNode<V> newLeaf = leaf.insertEntry(key, value, insertPart);
      size++;
      if (!tryInPlaceLeafUpdate(stack, leaf, newLeaf)) {
         applyModification(stack, leaf, newLeaf);
      }
      checkModCount(mc);
      return null;
   }

   /**
    * Remove a key. Returns the removed value, or null if the key was not present.
    */
   public V remove(byte[] key) {
      int mc = modCount.incrementAndGet();
      Deque<PathEntry<V>> stack = new ArrayDeque<>();
      Node<V> node = root;
      while (node instanceof InnerNode<V> inner) {
         int idx = inner.getInsertionPoint(key);
         stack.push(new PathEntry<>(inner, idx));
         node = inner.children[idx].resolve();
      }
      LeafNode<V> leaf = (LeafNode<V>) node;

      int insertPart = leaf.getInsertionPoint(key);
      if (insertPart < 0) {
         checkModCount(mc);
         return null;
      }

      V old = leaf.values[insertPart];
      LeafNode<V> newLeaf = leaf.removeEntry(insertPart);
      size--;
      if (!tryInPlaceLeafUpdate(stack, leaf, newLeaf)) {
         applyModification(stack, leaf, newLeaf);
      }
      checkModCount(mc);
      return old;
   }

   /** Removes all entries, resetting the tree to an empty root leaf. */
   public void clear() {
      modCount.incrementAndGet();
      root = new LeafNode<>(Util.EMPTY_BYTE_ARRAY_ARRAY, emptyValues(), INNER_NODE_HEADER_SIZE);
      size = 0;
   }

   private void checkModCount(int expected) {
      if (modCount.get() != expected) {
         throw new ConcurrentModificationException(
               "BPlusTree was concurrently modified by another writer");
      }
   }

   /**
    * Transforms a tree entry during {@link #publish} iteration. If the function returns
    * {@code null}, the entry is skipped and not emitted to the subscriber.
    */
   @FunctionalInterface
   public interface PublishFunction<V, R> {
      R apply(byte[] key, V value);
   }

   /**
    * Returns a {@link Flowable} that emits entries in sorted order. The function transforms each
    * entry, returning {@code null} to skip. Any exception thrown by the function or during node
    * resolution propagates as a Flowable error; subclasses may override to add retry logic.
    */
   public <R> Flowable<R> publish(PublishFunction<V, R> function) {
      return Flowable.defer(() -> {
         ByRef<byte[]> lastRetrievedKey = new ByRef<>(null);
         return new FlowableCreate<>(emitter -> {
            if (publishNode(root, lastRetrievedKey, emitter, function)) {
               emitter.onComplete();
            }
         }, BackpressureStrategy.ERROR);
      });
   }

   /**
    * Recursively walks the tree emitting entries via the function. Returns {@code true} if
    * the full iteration completed, {@code false} if stopped early due to cancellation or
    * backpressure.
    */
   <R> boolean publishNode(Node<V> node, ByRef<byte[]> lastRetrievedKey,
         FlowableEmitter<R> emitter, PublishFunction<V, R> function) {
      node = node.resolve();

      if (node instanceof LeafNode<V> leaf) {
         byte[] lastKey = lastRetrievedKey.get();
         int start = 0;
         if (lastKey != null && leaf.keys.length > 0) {
            start = leaf.getInsertionPoint(lastKey);
            if (start >= 0) {
               start++;
            } else {
               start = ~start;
            }
         }
         for (int i = start; i < leaf.keys.length; i++) {
            R result = function.apply(leaf.keys[i], leaf.values[i]);
            if (result != null) {
               emitter.onNext(result);
               if (emitter.requested() == 0 || emitter.isCancelled()) {
                  lastRetrievedKey.set(leaf.keys[i]);
                  return false;
               }
            }
            lastRetrievedKey.set(leaf.keys[i]);
         }
         return true;
      }

      InnerNode<V> inner = (InnerNode<V>) node;
      byte[] lastKey = lastRetrievedKey.get();
      int startChild = lastKey != null ? inner.getInsertionPoint(lastKey) : 0;
      for (int i = startChild; i < inner.children.length; i++) {
         if (!publishNode(inner.children[i], lastRetrievedKey, emitter, function)) {
            return false;
         }
      }
      return true;
   }

   // --- Rebalancing ---

   private void applyModification(Deque<PathEntry<V>> stack, Node<V> oldNode, Node<V> newNode) {
      ManageLengthResult<V> result = manageLength(stack, oldNode, newNode);
      if (result == null) {
         return;
      }
      for (;;) {
         if (stack.isEmpty()) {
            if (result.newNodes.size() == 1) {
               root = result.newNodes.get(0);
            } else {
               root = InnerNode.fromChildren(result.newNodes);
            }
            return;
         }
         PathEntry<V> path = stack.pop();
         onChildrenReplaced(path.node.children, result.from, result.to);
         InnerNode<V> parentCopy = path.node.copyWith(result.from, result.to, result.newNodes);
         result = manageLength(stack, path.node, parentCopy);
         if (result == null) {
            return;
         }
      }
   }

   private ManageLengthResult<V> manageLength(Deque<PathEntry<V>> stack, Node<V> oldNode, Node<V> newNode) {
      while (newNode instanceof InnerNode<V> inner && inner.children.length == 1) {
         newNode = inner.children[0].resolve();
      }
      int from, to;
      if ((newNode.length() < minNodeSize
            || (newNode instanceof LeafNode<V> ln && ln.keys.length == 0))
            && !stack.isEmpty()) {
         PathEntry<V> parent = stack.peek();
         if (parent.node.children.length == 1) {
            return new ManageLengthResult<>(0, 0, List.of(newNode));
         }
         int sizeWithLeft = Integer.MAX_VALUE;
         int sizeWithRight = Integer.MAX_VALUE;
         if (parent.index > 0) {
            sizeWithLeft = newNode.length() + parent.node.children[parent.index - 1].length() - INNER_NODE_HEADER_SIZE;
         }
         if (parent.index < parent.node.children.length - 1) {
            sizeWithRight = newNode.length() + parent.node.children[parent.index + 1].length() - INNER_NODE_HEADER_SIZE;
         }
         int joinWith;
         if (sizeWithLeft == Integer.MAX_VALUE) {
            joinWith = parent.index + 1;
         } else if (sizeWithRight == Integer.MAX_VALUE) {
            joinWith = parent.index - 1;
         } else if (sizeWithLeft > maxNodeSize && sizeWithRight > maxNodeSize) {
            joinWith = sizeWithLeft >= sizeWithRight ? parent.index - 1 : parent.index + 1;
         } else {
            joinWith = sizeWithLeft <= sizeWithRight ? parent.index - 1 : parent.index + 1;
         }
         Node<V> joiner = parent.node.children[joinWith].resolve();
         Node<V> joined;
         if (joinWith < parent.index) {
            joined = join(joiner, newNode.leftmostKey(), newNode);
            from = joinWith;
            to = parent.index;
         } else {
            joined = join(newNode, joiner.leftmostKey(), joiner);
            from = parent.index;
            to = joinWith;
         }
         if (joined.length() <= maxNodeSize) {
            return new ManageLengthResult<>(from, to, List.of(joined));
         } else {
            return new ManageLengthResult<>(from, to, joined.split(maxNodeSize));
         }
      } else if (newNode == oldNode) {
         return null;
      } else if (!stack.isEmpty()) {
         PathEntry<V> parent = stack.peek();
         from = to = parent.index;
      } else {
         if (newNode.length() <= maxNodeSize) {
            root = newNode;
            return null;
         }
         from = to = 0;
      }
      if (newNode.length() <= maxNodeSize) {
         return new ManageLengthResult<>(from, to, List.of(newNode));
      }
      return new ManageLengthResult<>(from, to, newNode.split(maxNodeSize));
   }

   @SuppressWarnings("unchecked")
   private static <V> Node<V> join(Node<V> left, byte[] middleKey, Node<V> right) {
      // If either leaf side is empty, just return the other
      if (left instanceof LeafNode<V> leftLeaf && leftLeaf.keys.length == 0) {
         return right;
      }
      if (right instanceof LeafNode<V> rightLeaf && rightLeaf.keys.length == 0) {
         return left;
      }

      if (left instanceof InnerNode<V> leftInner && right instanceof InnerNode<V> rightInner) {
         byte[] newPrefix = commonPrefix(leftInner.prefix, rightInner.prefix);
         newPrefix = commonPrefix(newPrefix, middleKey);
         byte[][] newKeyParts = new byte[leftInner.keyParts.length + rightInner.keyParts.length + 1][];
         copyKeyParts(leftInner.keyParts, 0, newKeyParts, 0, leftInner.keyParts.length, leftInner.prefix, newPrefix);
         byte[] rightmostKey = left.rightmostKey();
         int commonLength = Math.abs(compare(middleKey, rightmostKey));
         newKeyParts[leftInner.keyParts.length] = substring(middleKey, newPrefix.length, commonLength);
         copyKeyParts(rightInner.keyParts, 0, newKeyParts, leftInner.keyParts.length + 1, rightInner.keyParts.length, rightInner.prefix, newPrefix);
         Node<V>[] newChildren = new Node[leftInner.children.length + rightInner.children.length];
         System.arraycopy(leftInner.children, 0, newChildren, 0, leftInner.children.length);
         System.arraycopy(rightInner.children, 0, newChildren, leftInner.children.length, rightInner.children.length);
         return new InnerNode<>(newPrefix, newKeyParts, newChildren);
      } else if (left instanceof LeafNode<V> lLeaf && right instanceof LeafNode<V> rLeaf) {
         byte[][] newKeys = new byte[lLeaf.keys.length + rLeaf.keys.length][];
         System.arraycopy(lLeaf.keys, 0, newKeys, 0, lLeaf.keys.length);
         System.arraycopy(rLeaf.keys, 0, newKeys, lLeaf.keys.length, rLeaf.keys.length);
         V[] newValues = (V[]) new Object[lLeaf.values.length + rLeaf.values.length];
         System.arraycopy(lLeaf.values, 0, newValues, 0, lLeaf.values.length);
         System.arraycopy(rLeaf.values, 0, newValues, lLeaf.values.length, rLeaf.values.length);
         return LeafNode.createLeaf(newKeys, newValues);
      } else {
         throw new IllegalArgumentException("Cannot join inner and leaf nodes");
      }
   }

   @SuppressWarnings("unchecked")
   private V[] newArray(V value) {
      V[] arr = (V[]) new Object[1];
      arr[0] = value;
      return arr;
   }

   // --- Node classes ---

   protected abstract static class Node<V> {
      /**
       * Returns the concrete node that holds the actual tree data. Subclasses that act as
       * lazy wrappers (e.g. disk-backed soft-referenced nodes) must override this to load
       * and return the underlying node. Callers must always invoke {@code resolve()} before
       * accessing any fields or performing traversal on a node, since the node instance
       * itself may be a placeholder whose fields do not contain real tree data.
       */
      Node<V> resolve() {
         return this;
      }

      abstract int getInsertionPoint(byte[] key);
      abstract int length();
      abstract byte[] leftmostKey();
      abstract byte[] rightmostKey();
      abstract List<Node<V>> split(int maxNodeSize);
   }

   static final class InnerNode<V> extends Node<V> {
      final byte[] prefix;
      final byte[][] keyParts;
      final Node<V>[] children;

      InnerNode(byte[] prefix, byte[][] keyParts, Node<V>[] children) {
         this.prefix = prefix;
         this.keyParts = keyParts;
         this.children = children;
      }

      @Override
      int getInsertionPoint(byte[] key) {
         int comp = compare(key, prefix, prefix.length);
         if (comp > 0) {
            return 0;
         } else if (comp < 0) {
            return keyParts.length;
         } else {
            byte[] keyPostfix = substring(key, prefix.length, key.length);
            int insertionPoint = Arrays.binarySearch(keyParts, keyPostfix, REVERSED_COMPARE_TO);
            if (insertionPoint < 0) {
               insertionPoint = -insertionPoint - 1;
            } else {
               insertionPoint += 1;
            }
            return insertionPoint;
         }
      }

      int headerLength() {
         return INNER_NODE_HEADER_SIZE + prefix.length;
      }

      int keyPartsLength() {
         int sum = 0;
         for (byte[] keyPart : keyParts) {
            sum += 2 + keyPart.length;
         }
         return sum;
      }

      @SuppressWarnings("unchecked")
      static <V> InnerNode<V> fromChildren(List<Node<V>> nodes) {
         byte[][] newKeys = new byte[nodes.size() - 1][];
         byte[] newPrefix = null;
         for (int i = 0; i < newKeys.length; i++) {
            Node<V> n = nodes.get(i + 1);
            newKeys[i] = n.leftmostKey();
            assert newKeys[i] != null : "Node at index " + (i + 1) + " has null leftmostKey";
            if (newPrefix == null) {
               newPrefix = newKeys[i];
            } else {
               newPrefix = commonPrefix(newPrefix, newKeys[i]);
            }
         }
         if (newPrefix == null) {
            newPrefix = Util.EMPTY_BYTE_ARRAY;
         }
         byte[][] adjustedKeys = new byte[newKeys.length][];
         for (int i = 0; i < newKeys.length; i++) {
            adjustedKeys[i] = substring(newKeys[i], newPrefix.length, newKeys[i].length);
         }
         Node<V>[] childArray = new Node[nodes.size()];
         nodes.toArray(childArray);
         return new InnerNode<>(newPrefix, adjustedKeys, childArray);
      }

      @SuppressWarnings("unchecked")
      InnerNode<V> copyWith(int oldFrom, int oldTo, List<Node<V>> newNodes) {
         Node<V>[] newChildren = new Node[children.length + newNodes.size() - 1 - oldTo + oldFrom];
         System.arraycopy(children, 0, newChildren, 0, oldFrom);
         System.arraycopy(children, oldTo + 1, newChildren, oldFrom + newNodes.size(), children.length - oldTo - 1);
         for (int i = 0; i < newNodes.size(); i++) {
            newChildren[i + oldFrom] = newNodes.get(i);
         }
         byte[][] newKeys = new byte[newNodes.size() - 1][];
         byte[] newPrefix = prefix;
         byte[] firstNewLeftmost = oldFrom > 0 ? newNodes.get(0).leftmostKey() : null;
         if (firstNewLeftmost != null) {
            newPrefix = commonPrefix(newPrefix, firstNewLeftmost);
         }
         for (int i = 0; i < newKeys.length; i++) {
            newKeys[i] = newNodes.get(i + 1).leftmostKey();
            if (newKeys[i] == null) {
               throw new IllegalStateException("Node at index " + (i + 1) + " has null leftmostKey: " + newNodes.get(i + 1));
            }
            newPrefix = commonPrefix(newPrefix, newKeys[i]);
         }
         byte[][] newKeyParts = new byte[keyParts.length + newNodes.size() - 1 - oldTo + oldFrom][];
         int keptBefore = oldFrom > 0 ? oldFrom - 1 : 0;
         copyKeyParts(keyParts, 0, newKeyParts, 0, keptBefore, prefix, newPrefix);
         if (oldFrom > 0) {
            if (firstNewLeftmost != null) {
               newKeyParts[oldFrom - 1] = substring(firstNewLeftmost, newPrefix.length, firstNewLeftmost.length);
            } else {
               copyKeyParts(keyParts, oldFrom - 1, newKeyParts, oldFrom - 1, 1, prefix, newPrefix);
            }
         }
         copyKeyParts(keyParts, oldTo, newKeyParts, oldFrom + newKeys.length, keyParts.length - oldTo, prefix, newPrefix);
         for (int i = 0; i < newKeys.length; i++) {
            newKeyParts[i + oldFrom] = substring(newKeys[i], newPrefix.length, newKeys[i].length);
         }
         return new InnerNode<>(newPrefix, newKeyParts, newChildren);
      }

      @Override
      int length() {
         return headerLength() + keyPartsLength() + INNER_NODE_REFERENCE_SIZE * children.length;
      }

      @Override
      byte[] leftmostKey() {
         for (Node<V> child : children) {
            byte[] key = child.leftmostKey();
            if (key != null) return key;
         }
         return null;
      }

      @Override
      byte[] rightmostKey() {
         for (int i = children.length - 1; i >= 0; i--) {
            byte[] key = children[i].rightmostKey();
            if (key != null) return key;
         }
         return null;
      }

      @Override
      @SuppressWarnings("unchecked")
      List<Node<V>> split(int maxNodeSize) {
         return splitNode(prefix, keyParts, maxNodeSize,
               (newPrefix, newKeyParts, childFrom, childTo) -> {
                  Node<V>[] newChildren = new Node[childTo - childFrom + 1];
                  System.arraycopy(children, childFrom, newChildren, 0, childTo - childFrom + 1);
                  return new InnerNode<>(newPrefix, newKeyParts, newChildren);
               });
      }
   }

   protected static final class LeafNode<V> extends Node<V> {
      final byte[][] keys;
      final V[] values;
      final int valuesOffset;

      LeafNode(byte[][] keys, V[] values, int valuesOffset) {
         this.keys = keys;
         this.values = values;
         this.valuesOffset = valuesOffset;
      }

      static <V> LeafNode<V> createLeaf(byte[][] keys, V[] values) {
         return new LeafNode<>(keys, values, INNER_NODE_HEADER_SIZE);
      }

      @Override
      int getInsertionPoint(byte[] key) {
         return Arrays.binarySearch(keys, key, REVERSED_COMPARE_TO);
      }

      @SuppressWarnings("unchecked")
      LeafNode<V> insertEntry(byte[] key, V value, int insertionPoint) {
         byte[][] newKeys = new byte[keys.length + 1][];
         V[] newValues = (V[]) new Object[values.length + 1];
         System.arraycopy(keys, 0, newKeys, 0, insertionPoint);
         newKeys[insertionPoint] = key;
         System.arraycopy(keys, insertionPoint, newKeys, insertionPoint + 1, keys.length - insertionPoint);
         System.arraycopy(values, 0, newValues, 0, insertionPoint);
         newValues[insertionPoint] = value;
         System.arraycopy(values, insertionPoint, newValues, insertionPoint + 1, values.length - insertionPoint);
         return createLeaf(newKeys, newValues);
      }

      @SuppressWarnings("unchecked")
      LeafNode<V> removeEntry(int index) {
         byte[][] newKeys = new byte[keys.length - 1][];
         V[] newValues = (V[]) new Object[values.length - 1];
         System.arraycopy(keys, 0, newKeys, 0, index);
         System.arraycopy(keys, index + 1, newKeys, index, newKeys.length - index);
         System.arraycopy(values, 0, newValues, 0, index);
         System.arraycopy(values, index + 1, newValues, index, newValues.length - index);
         return createLeaf(newKeys, newValues);
      }

      @Override
      int length() {
         return valuesOffset + LEAF_NODE_REFERENCE_SIZE * values.length;
      }

      @Override
      byte[] leftmostKey() {
         return keys.length > 0 ? keys[0] : null;
      }

      @Override
      byte[] rightmostKey() {
         return keys.length > 0 ? keys[keys.length - 1] : null;
      }

      @Override
      List<Node<V>> split(int maxNodeSize) {
         if (values.length <= 1) {
            return List.of(this);
         }
         int maxPerLeaf = (maxNodeSize - INNER_NODE_HEADER_SIZE) / LEAF_NODE_REFERENCE_SIZE;
         if (maxPerLeaf < 1) maxPerLeaf = 1;
         if (values.length <= maxPerLeaf) {
            return List.of(this);
         }
         List<Node<V>> result = new ArrayList<>();
         for (int i = 0; i < values.length; i += maxPerLeaf) {
            int end = Math.min(i + maxPerLeaf, values.length);
            result.add(createSubLeaf(i, end));
         }
         return result;
      }

      @SuppressWarnings("unchecked")
      private LeafNode<V> createSubLeaf(int from, int toExclusive) {
         int count = toExclusive - from;
         byte[][] newKeys = new byte[count][];
         System.arraycopy(keys, from, newKeys, 0, count);
         V[] newValues = (V[]) new Object[count];
         System.arraycopy(values, from, newValues, 0, count);
         return createLeaf(newKeys, newValues);
      }
   }

   // --- Internal types ---

   protected record PathEntry<V>(InnerNode<V> node, int index) {
   }

   private record ManageLengthResult<V>(int from, int to, List<Node<V>> newNodes) {
   }

   // --- Key comparison ---
   // Positive result: first sorts before second. Magnitude = mismatch position + 1.
   // Zero: equal (or first is a prefix match of second up to secondLength).

   static int compare(byte[] first, byte[] second) {
      int mismatchPos = Arrays.mismatch(first, second);
      if (mismatchPos == -1) {
         return 0;
      } else if (mismatchPos >= first.length) {
         return first.length + 1;
      } else if (mismatchPos >= second.length) {
         return -second.length - 1;
      }
      return second[mismatchPos] > first[mismatchPos] ? mismatchPos + 1 : -mismatchPos - 1;
   }

   static int compare(byte[] first, byte[] second, int secondLength) {
      if (secondLength == 0) {
         return 0;
      }
      int mismatchPos = Arrays.mismatch(first, 0, first.length, second, 0, secondLength);
      if (mismatchPos == -1 || mismatchPos == secondLength) {
         return 0;
      } else if (mismatchPos >= first.length) {
         return first.length + 1;
      }
      return second[mismatchPos] > first[mismatchPos] ? mismatchPos + 1 : -mismatchPos - 1;
   }

   static final Comparator<byte[]> REVERSED_COMPARE_TO = ((Comparator<byte[]>) BPlusTree::compare).reversed();

   // --- Byte array utilities ---

   static byte[] concat(byte[] first, byte[] second) {
      if (first == null || first.length == 0) return second;
      if (second == null || second.length == 0) return first;
      byte[] result = new byte[first.length + second.length];
      System.arraycopy(first, 0, result, 0, first.length);
      System.arraycopy(second, 0, result, first.length, second.length);
      return result;
   }

   static byte[] substring(byte[] key, int begin, int end) {
      if (end <= begin) return Util.EMPTY_BYTE_ARRAY;
      if (begin == 0 && end == key.length) {
         return key;
      }
      byte[] sub = new byte[end - begin];
      System.arraycopy(key, begin, sub, 0, end - begin);
      return sub;
   }

   static byte[] commonPrefix(byte[] oldPrefix, byte[] newKey) {
      int i = Arrays.mismatch(oldPrefix, newKey);
      if (i == -1 || i == oldPrefix.length) {
         return oldPrefix;
      }
      if (i == newKey.length) {
         return newKey;
      }
      if (i == 0) {
         return Util.EMPTY_BYTE_ARRAY;
      }
      byte[] prefix = new byte[i];
      System.arraycopy(oldPrefix, 0, prefix, 0, i);
      return prefix;
   }

   static void copyKeyParts(byte[][] src, int srcIndex, byte[][] dest, int destIndex, int length,
                              byte[] oldPrefix, byte[] common) {
      if (oldPrefix.length == common.length) {
         System.arraycopy(src, srcIndex, dest, destIndex, length);
      } else {
         for (int i = 0; i < length; i++) {
            byte[] oldKeyPart = src[srcIndex + i];
            byte[] newPart = new byte[oldKeyPart.length + oldPrefix.length - common.length];
            System.arraycopy(oldPrefix, common.length, newPart, 0, oldPrefix.length - common.length);
            System.arraycopy(oldKeyPart, 0, newPart, oldPrefix.length - common.length, oldKeyPart.length);
            dest[destIndex + i] = newPart;
         }
      }
   }

   // --- Split algorithm ---

   @FunctionalInterface
   interface SubNodeFactory<V> {
      Node<V> create(byte[] newPrefix, byte[][] newKeyParts, int childFrom, int childTo);
   }

   static <V> List<Node<V>> splitNode(byte[] prefix, byte[][] keyParts,
                                      int maxLength, SubNodeFactory<V> factory) {
      int headerLength = INNER_NODE_HEADER_SIZE + prefix.length;
      int keyPartsLen = 0;
      for (byte[] kp : keyParts) {
         keyPartsLen += 2 + kp.length;
      }
      int contentLength = keyPartsLen + BPlusTree.INNER_NODE_REFERENCE_SIZE * (keyParts.length + 1);
      int targetParts = contentLength / Math.max(maxLength - headerLength, 1) + 1;
      int targetLength = contentLength / targetParts + headerLength;
      List<Node<V>> list = new ArrayList<>();
      byte[] prefixExtension = keyParts[0];
      int currentLength = INNER_NODE_HEADER_SIZE + prefix.length + prefixExtension.length + 2 * BPlusTree.INNER_NODE_REFERENCE_SIZE + 2;
      int nodeFrom = 0;
      for (int i = 1; i < keyParts.length; i++) {
         int newLength;
         byte[] newPrefixExtension = commonPrefix(prefixExtension, keyParts[i]);
         if (newPrefixExtension.length != prefixExtension.length) {
            newLength = currentLength + (prefixExtension.length - newPrefixExtension.length) * (i - nodeFrom - 1);
         } else {
            newLength = currentLength;
         }
         newLength += keyParts[i].length - newPrefixExtension.length + BPlusTree.INNER_NODE_REFERENCE_SIZE + 2;
         if (newLength < targetLength) {
            currentLength = newLength;
         } else {
            Node<V> subNode;
            if (newLength > maxLength) {
               subNode = createSubNode(prefix, keyParts, factory, prefixExtension, nodeFrom, i);
               ++i;
            } else {
               subNode = createSubNode(prefix, keyParts, factory, newPrefixExtension, nodeFrom, i + 1);
               i += 2;
            }
            list.add(subNode);
            if (i < keyParts.length) {
               newPrefixExtension = keyParts[i];
            }
            currentLength = INNER_NODE_HEADER_SIZE + prefix.length + newPrefixExtension.length + 2 * BPlusTree.INNER_NODE_REFERENCE_SIZE + 2;
            nodeFrom = i;
         }
         prefixExtension = newPrefixExtension;
      }
      if (nodeFrom <= keyParts.length) {
         list.add(createSubNode(prefix, keyParts, factory, prefixExtension, nodeFrom, keyParts.length));
      }
      return list;
   }

   private static <V> Node<V> createSubNode(byte[] prefix, byte[][] keyParts, SubNodeFactory<V> factory,
         byte[] newPrefixExtension, int childFrom, int childTo) {
      byte[][] newKeyParts = new byte[childTo - childFrom][];
      if (newPrefixExtension.length > 0) {
         for (int i = childFrom; i < childTo; i++) {
            newKeyParts[i - childFrom] = substring(keyParts[i], newPrefixExtension.length, keyParts[i].length);
         }
      } else {
         System.arraycopy(keyParts, childFrom, newKeyParts, 0, childTo - childFrom);
      }
      byte[] newPrefix = childFrom == childTo ? Util.EMPTY_BYTE_ARRAY : concat(prefix, newPrefixExtension);
      return factory.create(newPrefix, newKeyParts, childFrom, childTo);
   }
}
