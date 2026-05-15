package org.infinispan.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commons.util.ByRef;
import org.infinispan.reactive.FlowableCreate;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;

/**
 * A {@link BPlusTree} subclass that persists each node independently to a
 * {@link NodeStore} and wraps every inner-node child in a {@link SoftNode}.
 * <p>
 * After each {@link #put} or {@link #remove}, only the nodes on the modified
 * path (from the affected leaf up to the root's children) are serialized and
 * written — typically O(height) writes of a few hundred bytes each. Unmodified
 * subtrees keep their existing disk locations. A {@link SoftReference} at every
 * inner-node level lets the JVM evict cached subtrees under memory pressure;
 * they are reloaded from disk on demand.
 * <p>
 * Keys are not stored on disk. On deserialization, keys are loaded from the
 * external data source via the {@link KeyLoader} callback. This matches the
 * old {@code IndexNode} disk format exactly.
 * <p>
 * Disk space is managed internally using a block-aligned free list. Freed
 * blocks are pooled by size and reused for future allocations to avoid
 * unbounded index file growth.
 *
 * @param <V> value type
 */
public class SoftBPlusTree<V> extends BPlusTree<V> {
   static final byte HAS_LEAVES = 1;
   static final byte HAS_NODES = 2;
   private static final int MAX_OUTDATED_RETRIES = 10;

   private final NodeStore store;
   private final ValueSerializer<V> serializer;
   private final KeyLoader<V> keyLoader;
   private final List<NodeSpace> pendingFrees = new ArrayList<>();
   private final ByteBuffer serializeBuffer;
   private final short blockAlignment;
   private final long initialStoreSize;
   private long storeSize;
   @SuppressWarnings("unchecked")
   private List<NodeSpace>[] freeBlocks = new List[0];

   /**
    * Creates a disk-backed B+ tree with block-aligned free space management.
    *
    * @param minNodeSize     minimum serialized node size before merging
    * @param maxNodeSize     maximum serialized node size before splitting
    * @param store           backing storage for serialized nodes
    * @param serializer      value serializer/deserializer
    * @param keyLoader       reconstructs keys from values during deserialization
    * @param blockAlignment  disk block alignment in bytes (freed blocks are pooled by aligned size)
    * @param initialStoreSize starting byte offset for new allocations (typically the header size)
    */
   public SoftBPlusTree(int minNodeSize, int maxNodeSize, NodeStore store,
                        ValueSerializer<V> serializer, KeyLoader<V> keyLoader,
                        short blockAlignment, long initialStoreSize) {
      super(minNodeSize, maxNodeSize);
      this.store = store;
      this.serializer = serializer;
      this.keyLoader = keyLoader;
      this.serializeBuffer = ByteBuffer.allocate(maxNodeSize);
      this.blockAlignment = blockAlignment;
      this.initialStoreSize = initialStoreSize;
      this.storeSize = initialStoreSize;
   }

   /**
    * Creates a disk-backed B+ tree with no block alignment (alignment of 1) and an
    * initial store size of 0.
    *
    * @see #SoftBPlusTree(int, int, NodeStore, ValueSerializer, KeyLoader, short, long)
    */
   public SoftBPlusTree(int minNodeSize, int maxNodeSize, NodeStore store,
                        ValueSerializer<V> serializer, KeyLoader<V> keyLoader) {
      this(minNodeSize, maxNodeSize, store, serializer, keyLoader, (short) 1, 0);
   }

   // --- Space management ---

   private short alignBlock(short length) {
      if (blockAlignment <= 1) return length;
      return (short) ((length + (blockAlignment - 1)) & -blockAlignment);
   }

   private int bucketIndex(short aligned) {
      return (aligned / blockAlignment) - 1;
   }

   @SuppressWarnings("unchecked")
   private void ensureBucketCapacity(int idx) {
      if (idx >= freeBlocks.length) {
         List<NodeSpace>[] newArr = new List[idx + 1];
         System.arraycopy(freeBlocks, 0, newArr, 0, freeBlocks.length);
         freeBlocks = newArr;
      }
   }

   private NodeSpace allocate(short length) {
      short aligned = alignBlock(length);
      int idx = bucketIndex(aligned);
      int maxIdx = bucketIndex((short) (aligned + (aligned >> 2)));
      if (maxIdx >= freeBlocks.length) maxIdx = freeBlocks.length - 1;
      for (int i = idx; i <= maxIdx; i++) {
         List<NodeSpace> list = freeBlocks[i];
         if (list != null && !list.isEmpty()) {
            return list.remove(list.size() - 1);
         }
      }
      long offset = storeSize;
      storeSize += aligned;
      return new NodeSpace(offset, aligned);
   }

   private void freeBlock(long offset, short occupiedSpace) {
      if (offset + occupiedSpace == storeSize) {
         storeSize = offset;
      } else {
         int idx = bucketIndex(occupiedSpace);
         ensureBucketCapacity(idx);
         List<NodeSpace> list = freeBlocks[idx];
         if (list == null) {
            list = new ArrayList<>();
            freeBlocks[idx] = list;
         }
         list.add(new NodeSpace(offset, occupiedSpace));
      }
   }

   /** Returns the current logical size of the backing store in bytes. */
   public long getStoreSize() {
      return storeSize;
   }

   /** Sets the logical size of the backing store, typically restored from a persisted header. */
   public void setStoreSize(long size) {
      this.storeSize = size;
   }

   /**
    * Serializes the free block lists into a {@link ByteBuffer} for persistence. The caller
    * should write this alongside the tree root descriptor so that free space can be restored
    * on restart via {@link #deserializeFreeBlocks(ByteBuffer)}.
    *
    * @return a flipped buffer containing the serialized free block state
    */
   public ByteBuffer serializeFreeBlocks() {
      int numNonEmpty = 0;
      int size = 4;
      for (List<NodeSpace> list : freeBlocks) {
         if (list != null && !list.isEmpty()) {
            numNonEmpty++;
            size += 8 + list.size() * 10;
         }
      }
      ByteBuffer buf = ByteBuffer.allocate(size);
      buf.putInt(numNonEmpty);
      for (int i = 0; i < freeBlocks.length; i++) {
         List<NodeSpace> list = freeBlocks[i];
         if (list != null && !list.isEmpty()) {
            buf.putInt((i + 1) * blockAlignment);
            buf.putInt(list.size());
            for (NodeSpace ns : list) {
               buf.putLong(ns.offset());
               buf.putShort(ns.occupiedSpace());
            }
         }
      }
      buf.flip();
      return buf;
   }

   /**
    * Restores the free block lists from a buffer previously produced by
    * {@link #serializeFreeBlocks()}. Any existing free block state is replaced.
    */
   @SuppressWarnings("unchecked")
   public void deserializeFreeBlocks(ByteBuffer buf) {
      freeBlocks = new List[0];
      int numLists = buf.getInt();
      for (int i = 0; i < numLists; i++) {
         int blockLength = buf.getInt();
         int listSize = buf.getInt();
         if (listSize > 0) {
            int idx = bucketIndex((short) blockLength);
            ensureBucketCapacity(idx);
            ArrayList<NodeSpace> list = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; j++) {
               list.add(new NodeSpace(buf.getLong(), buf.getShort()));
            }
            freeBlocks[idx] = list;
         }
      }
   }

   // --- Tree hooks ---

   @Override
   protected void onChildrenReplaced(Node<V>[] oldChildren, int from, int to) {
      for (int i = from; i <= to; i++) {
         collectFreedNodes(oldChildren[i]);
      }
   }

   private void collectFreedNodes(Node<V> node) {
      if (node instanceof SoftNode<V> soft) {
         pendingFrees.add(new NodeSpace(soft.diskOffset, soft.occupiedSpace));
      }
   }

   @Override
   protected void onValueOverwritten(Deque<PathEntry<V>> stack, LeafNode<V> leaf,
         int entryIndex) {
      if (stack.isEmpty()) return;
      PathEntry<V> parent = stack.peek();
      Node<V> childNode = parent.node().children[parent.index()];
      if (!(childNode instanceof SoftNode<V> softNode)) return;
      int valueSize = serializer.serializedSize(leaf.values[entryIndex]);
      int valueOffset = leaf.valuesOffset + entryIndex * valueSize;
      ByteBuffer data;
      if (valueSize <= serializeBuffer.capacity()) {
         serializeBuffer.clear().limit(valueSize);
         data = serializeBuffer;
      } else {
         data = ByteBuffer.allocate(valueSize);
      }
      serializer.write(leaf.values[entryIndex], data);
      data.flip();
      try {
         store.write(data, softNode.diskOffset + valueOffset);
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   @Override
   protected boolean tryInPlaceLeafUpdate(Deque<PathEntry<V>> stack, LeafNode<V> oldLeaf,
         LeafNode<V> newLeaf) {
      if (stack.isEmpty()) return false;

      if (newLeaf.keys.length == 0) return false;

      if (newLeaf.length() < minNodeSize || newLeaf.length() > maxNodeSize) {
         return false;
      }

      PathEntry<V> parentEntry = stack.peek();
      Node<V> childNode = parentEntry.node().children[parentEntry.index()];
      if (!(childNode instanceof SoftNode<V> oldSoftNode)) {
         return false;
      }

      if (parentEntry.index() > 0) {
         byte[] oldLeft = oldLeaf.leftmostKey();
         byte[] newLeft = newLeaf.leftmostKey();
         if (!Arrays.equals(oldLeft, newLeft)) {
            return false;
         }
      }

      try {
         NodeSpace newSpace = writeNodeToStore(newLeaf);

         pendingFrees.add(new NodeSpace(oldSoftNode.diskOffset, oldSoftNode.occupiedSpace));

         SoftNode<V> newSoftNode = new SoftNode<>(newLeaf, newSpace.offset, newSpace.occupiedSpace, this);
         parentEntry.node().children[parentEntry.index()] = newSoftNode;

         Iterator<PathEntry<V>> it = stack.iterator();
         it.next();
         if (it.hasNext()) {
            PathEntry<V> grandparentEntry = it.next();
            SoftNode<V> parentSoftNode = (SoftNode<V>) grandparentEntry.node().children[grandparentEntry.index()];
            int refOffset = parentEntry.node().headerLength() + parentEntry.node().keyPartsLength()
                  + parentEntry.index() * INNER_NODE_REFERENCE_SIZE;
            ByteBuffer refData = ByteBuffer.allocate(INNER_NODE_REFERENCE_SIZE);
            refData.putLong(newSpace.offset);
            refData.putShort(newSpace.occupiedSpace);
            refData.flip();
            store.write(refData, parentSoftNode.diskOffset + refOffset);
         }

         return true;
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   /**
    * {@inheritDoc}
    * <p>
    * Retries up to {@code MAX_OUTDATED_RETRIES} times on {@link IndexNodeOutdatedException},
    * which can occur when a {@link SoftNode} resolves from disk and the backing data has been
    * concurrently deleted (e.g., by a compactor).
    */
   @Override
   public V get(byte[] key) {
      for (int attempts = 0; ; attempts++) {
         try {
            return super.get(key);
         } catch (IndexNodeOutdatedException e) {
            if (attempts >= MAX_OUTDATED_RETRIES) {
               throw e;
            }
         }
      }
   }

   @Override
   public V put(byte[] key, V value) {
      try {
         V old = super.put(key, value);
         afterMutation();
         return old;
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   @Override
   public V remove(byte[] key) {
      try {
         V old = super.remove(key);
         afterMutation();
         return old;
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   /**
    * {@inheritDoc}
    * <p>
    * Retries the entire iteration up to {@code MAX_OUTDATED_RETRIES} times on
    * {@link IndexNodeOutdatedException}, resuming from the last successfully emitted key.
    * The function may throw {@link IndexNodeOutdatedException} to force a retry for the
    * current entry rather than returning {@code null} (which skips the entry without retrying).
    */
   @Override
   public <R> Flowable<R> publish(PublishFunction<V, R> function) {
      return publishWithRetry(function);
   }

   /**
    * Returns a {@link Flowable} that emits entries in sorted order with
    * {@link IndexNodeOutdatedException} handling controlled by {@code skipOnOutdated}.
    * <p>
    * When {@code skipOnOutdated} is {@code true}, any {@link IndexNodeOutdatedException}
    * thrown by the function is caught and the entry is silently skipped (treated as if the
    * function returned {@code null}). Exceptions thrown during node resolution still trigger
    * a full retry. When {@code false}, all {@link IndexNodeOutdatedException}s trigger a retry.
    */
   public <R> Flowable<R> publish(IOPublishFunction<V, R> function, boolean skipOnOutdated) {
      if (!skipOnOutdated) {
         return publishWithRetry(function);
      }
      return publishWithRetry((k, v) -> {
         try {
            return function.apply(k, v);
         } catch (IndexNodeOutdatedException e) {
            return null;
         }
      });
   }

   private <R> Flowable<R> publishWithRetry(PublishFunction<V, R> function) {
      return Flowable.defer(() -> {
         ByRef<byte[]> lastRetrievedKey = new ByRef<>(null);
         return new FlowableCreate<>(emitter -> {
            int retries = 0;
            for (;;) {
               try {
                  if (publishNode(getRoot(), lastRetrievedKey, emitter, function)) {
                     emitter.onComplete();
                  }
                  return;
               } catch (IndexNodeOutdatedException e) {
                  if (emitter.isCancelled()) return;
                  if (++retries > MAX_OUTDATED_RETRIES) {
                     emitter.onError(new IllegalStateException(
                           "Exceeded maximum retry attempts (" + MAX_OUTDATED_RETRIES + ") during publish", e));
                     return;
                  }
               }
            }
         }, BackpressureStrategy.ERROR);
      });
   }

   /**
    * {@inheritDoc}
    * <p>
    * Also resets the free block lists, restores the store size to its initial value,
    * and truncates the backing store.
    */
   @Override
   @SuppressWarnings("unchecked")
   public void clear() {
      super.clear();
      pendingFrees.clear();
      freeBlocks = new List[0];
      storeSize = initialStoreSize;
      try {
         store.truncate(0);
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   /**
    * Persists the tree to the store. All descendants should already be persisted
    * as SoftNodes (via afterMutation calls during normal operation). This method
    * serializes the root node itself and returns a descriptor that the caller can
    * write into the file header for {@link #loadTree} to find on restart.
    * <p>
    * The free-block state must be persisted separately by the caller via
    * {@link #serializeFreeBlocks()}.
    *
    * @return the root's disk location, or null if the tree is empty
    */
   public NodeSpace saveTree() throws IOException {
      Node<V> r = getRoot();
      if (r instanceof LeafNode<V> leaf && leaf.values.length == 0) {
         return null;
      }
      if (r instanceof InnerNode<V> inner) {
         persistNewNodes(inner);
      }
      return writeNodeToStore(r);
   }

   /**
    * Reconstructs the tree from a previously persisted root. For an inner root,
    * each child is created as a {@link SoftNode} pointing at its disk location —
    * no child data is loaded until first access.
    */
   public void loadTree(NodeSpace rootSpace) throws IOException {
      if (rootSpace == null) {
         return;
      }
      ByteBuffer data = store.read(rootSpace.offset, rootSpace.occupiedSpace);
      Node<V> root = deserializeNode(data);
      setRoot(root);
   }

   private void afterMutation() throws IOException {
      for (NodeSpace ns : pendingFrees) {
         freeBlock(ns.offset, ns.occupiedSpace);
      }
      pendingFrees.clear();
      Node<V> r = getRoot();
      if (r instanceof InnerNode<V> inner) {
         persistNewNodes(inner);
      }
   }

   private void persistNewNodes(InnerNode<V> inner) throws IOException {
      for (int i = 0; i < inner.children.length; i++) {
         Node<V> child = inner.children[i];
         if (child instanceof SoftNode) continue;
         if (child instanceof InnerNode<V> childInner) {
            persistNewNodes(childInner);
         }
         NodeSpace space = writeNodeToStore(child);
         inner.children[i] = new SoftNode<>(child, space.offset, space.occupiedSpace, this);
      }
   }

   // Clearing the top-level SoftNode references is sufficient: when re-resolved from disk,
   // the deserialized InnerNode children are new SoftNodes with null references, so deeper
   // levels are effectively cleared lazily without needing to load them all from disk.
   void clearSoftReferences() {
      Node<V> r = getRoot();
      if (r instanceof InnerNode<V> inner) {
         for (Node<V> child : inner.children) {
            if (child instanceof SoftNode<V> soft) {
               soft.clearReference();
            }
         }
      }
   }

   /**
    * Thrown by a {@link PublishFunction} or during node resolution when the underlying data
    * for a tree entry is no longer available — for example, because a compactor deleted the
    * data file between the time the index was read and the time the entry was accessed.
    * <p>
    * {@link SoftBPlusTree} retries {@link #get}, {@link #put}, {@link #remove}, and
    * {@link #publish} up to {@code MAX_OUTDATED_RETRIES} times when this exception is thrown.
    * Callers should throw this instead of returning {@code null} when a missing resource
    * indicates a transient concurrent modification rather than a permanent error.
    */
   public static class IndexNodeOutdatedException extends RuntimeException {
      public IndexNodeOutdatedException(String message) {
         super(message);
      }
   }

   /**
    * A {@link PublishFunction} that may throw {@link IOException}. The IOException is
    * wrapped in {@link UncheckedIOException} before being passed to the base tree iteration.
    */
   @FunctionalInterface
   public interface IOPublishFunction<V, R> extends PublishFunction<V, R> {
      R applyIO(byte[] key, V value) throws IOException;

      @Override
      default R apply(byte[] key, V value) {
         try {
            return applyIO(key, value);
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      }
   }

   // --- Interfaces ---

   /**
    * Serializes and deserializes values stored in leaf nodes. Values are written
    * contiguously after the key-parts section of a serialized leaf node, so the
    * serialized size of every value must be fixed or self-describing.
    * <p>
    * <b>Important:</b> every value in a leaf must serialize to exactly
    * {@link #serializedSize} bytes. The tree relies on this for in-place
    * value overwrites — when a key's value is replaced, only that value's
    * bytes are rewritten at a calculated offset within the serialized node,
    * without re-serializing the entire node.
    *
    * @param <V> value type
    */
   public interface ValueSerializer<V> {
      /**
       * Writes the serialized form of {@code value} into {@code buffer} starting
       * at the buffer's current position. Must advance the position by exactly
       * {@link #serializedSize serializedSize(value)} bytes.
       */
      void write(V value, ByteBuffer buffer);

      /**
       * Reads a value from {@code buffer} starting at its current position.
       * Must advance the position by exactly the number of bytes that were
       * written by {@link #write} for this value.
       */
      V read(ByteBuffer buffer);

      /**
       * Returns the number of bytes that {@link #write} will produce for
       * {@code value}. Must be consistent with {@link #write} — the buffer
       * position must advance by exactly this amount.
       */
      int serializedSize(V value);
   }

   /**
    * Loads the B+ tree key for a given value during leaf node deserialization.
    * <p>
    * Keys are not stored in the serialized node format — only values are
    * persisted. When a leaf node is deserialized, each value is read via
    * {@link ValueSerializer#read} and then passed to this loader to reconstruct
    * the corresponding key. The returned key bytes are used for B+ tree lookups
    * and comparisons.
    * <p>
    * This callback is invoked once per value each time a leaf node is loaded
    * from disk (i.e., when a {@link SoftNode}'s soft reference has been cleared
    * and the node must be re-read).
    *
    * @param <V> value type
    */
   public interface KeyLoader<V> {
      /**
       * Returns the key bytes for the given value, or {@code null} if the
       * key cannot be loaded (e.g., the backing data file has been deleted).
       */
      byte[] loadKey(V value) throws IOException;
   }

   /**
    * Storage backend for serialized nodes. Provides raw I/O operations;
    * space allocation and free block management are handled by the tree.
    */
   public interface NodeStore {
      /**
       * Writes {@code data} (from position to limit) at the given offset.
       */
      void write(ByteBuffer data, long offset) throws IOException;

      /**
       * Reads {@code length} bytes from {@code offset}.
       */
      ByteBuffer read(long offset, int length) throws IOException;

      /**
       * Truncates the backing storage to the given size.
       */
      void truncate(long size) throws IOException;
   }

   /** A disk region descriptor: byte offset and allocated size. */
   public record NodeSpace(long offset, short occupiedSpace) { }

   // --- Serialization ---

   private NodeSpace writeNodeToStore(Node<V> node) throws IOException {
      serializeBuffer.clear();
      writeNode(node, serializer, serializeBuffer);
      serializeBuffer.flip();
      NodeSpace space = allocate((short) serializeBuffer.remaining());
      store.write(serializeBuffer, space.offset);
      return space;
   }

   static <V> ByteBuffer serializeNode(Node<V> node, ValueSerializer<V> serializer) {
      int size;
      if (node instanceof LeafNode<V> leaf) {
         size = leafSerializedSize(serializer, leaf.values);
      } else {
         size = innerNodeSerializedSize((InnerNode<V>) node);
      }
      ByteBuffer buffer = ByteBuffer.allocate(size);
      writeNode(node, serializer, buffer);
      buffer.flip();
      return buffer;
   }

   private static <V> void writeNode(Node<V> node, ValueSerializer<V> serializer, ByteBuffer buffer) {
      if (node instanceof LeafNode<V> leaf) {
         buffer.putShort((short) 0);
         buffer.put(HAS_LEAVES);
         buffer.putShort((short) leaf.values.length);
         for (int i = 0; i < leaf.values.length; i++) {
            serializer.write(leaf.values[i], buffer);
         }
      } else if (node instanceof InnerNode<V> inner) {
         buffer.putShort((short) inner.prefix.length);
         buffer.put(inner.prefix);
         buffer.put(HAS_NODES);
         buffer.putShort((short) inner.keyParts.length);
         for (byte[] keyPart : inner.keyParts) {
            buffer.putShort((short) keyPart.length);
            buffer.put(keyPart);
         }
         for (Node<V> child : inner.children) {
            if (child instanceof SoftNode<V> soft) {
               buffer.putLong(soft.diskOffset);
               buffer.putShort(soft.occupiedSpace);
            } else {
               throw new IllegalStateException("Inner node child must be SoftNode during serialization, got: " + child.getClass().getSimpleName());
            }
         }
      }
   }

   private static <V> int innerNodeSerializedSize(InnerNode<V> inner) {
      int size = 2 + inner.prefix.length + 1 + 2;
      for (byte[] keyPart : inner.keyParts) {
         size += 2 + keyPart.length;
      }
      size += INNER_NODE_REFERENCE_SIZE * inner.children.length;
      return size;
   }

   private static <V> int leafSerializedSize(ValueSerializer<V> serializer, V[] values) {
      int size = INNER_NODE_HEADER_SIZE;
      for (V value : values) {
         size += serializer.serializedSize(value);
      }
      return size;
   }

   @SuppressWarnings("unchecked")
   Node<V> deserializeNode(ByteBuffer buffer) {
      short prefixLen = buffer.getShort();
      byte[] prefix = new byte[prefixLen];
      buffer.get(prefix);
      byte flags = buffer.get();
      short count = buffer.getShort();
      if (flags == HAS_LEAVES) {
         try {
            V[] values = (V[]) new Object[count];
            byte[][] keys = new byte[count][];
            for (int i = 0; i < count; i++) {
               values[i] = serializer.read(buffer);
               keys[i] = keyLoader.loadKey(values[i]);
               if (keys[i] == null) {
                  throw new IndexNodeOutdatedException("KeyLoader returned null for value at index " + i);
               }
            }
            return new LeafNode<>(keys, values, INNER_NODE_HEADER_SIZE);
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
      } else {
         byte[][] keyParts = new byte[count][];
         for (int i = 0; i < count; i++) {
            short kpLen = buffer.getShort();
            keyParts[i] = new byte[kpLen];
            buffer.get(keyParts[i]);
         }
         int childCount = count + 1;
         Node<V>[] children = new Node[childCount];
         for (int i = 0; i < childCount; i++) {
            long offset = buffer.getLong();
            short occupiedSpace = buffer.getShort();
            children[i] = new SoftNode<>(offset, occupiedSpace, this);
         }
         return new InnerNode<>(prefix, keyParts, children);
      }
   }

   // --- SoftNode ---

   static final class SoftNode<V> extends Node<V> {
      private volatile SoftReference<Node<V>> reference;
      final long diskOffset;
      final short occupiedSpace;
      private final SoftBPlusTree<V> tree;
      private final ReentrantLock lock = new ReentrantLock();

      SoftNode(Node<V> actual, long diskOffset, short occupiedSpace, SoftBPlusTree<V> tree) {
         this.reference = new SoftReference<>(actual);
         this.diskOffset = diskOffset;
         this.occupiedSpace = occupiedSpace;
         this.tree = tree;
      }

      SoftNode(long diskOffset, short occupiedSpace, SoftBPlusTree<V> tree) {
         this.diskOffset = diskOffset;
         this.occupiedSpace = occupiedSpace;
         this.tree = tree;
      }

      @Override
      int getInsertionPoint(byte[] key) {
         return resolve().getInsertionPoint(key);
      }

      @Override
      Node<V> resolve() {
         Node<V> node;
         SoftReference<Node<V>> ref = this.reference;
         if (ref == null || (node = ref.get()) == null) {
            lock.lock();
            try {
               ref = this.reference;
               if (ref == null || (node = ref.get()) == null) {
                  ByteBuffer data = tree.store.read(diskOffset, occupiedSpace);
                  node = tree.deserializeNode(data);
                  this.reference = new SoftReference<>(node);
               }
            } catch (IOException e) {
               throw new UncheckedIOException(e);
            } finally {
               lock.unlock();
            }
         }
         return node;
      }

      @Override
      int length() {
         return resolve().length();
      }

      @Override
      byte[] leftmostKey() {
         return resolve().leftmostKey();
      }

      @Override
      byte[] rightmostKey() {
         return resolve().rightmostKey();
      }

      void clearReference() {
         this.reference = null;
      }

      @Override
      List<Node<V>> split(int maxNodeSize) {
         return resolve().split(maxNodeSize);
      }
   }
}
