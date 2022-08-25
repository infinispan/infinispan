package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.reactive.FlowableCreate;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;

/**
 * The recursive index structure. References to children are held in soft references,
 * which allows JVM-handled caching and reduces the amount of reads required while
 * evading OOMs if the index gets too big.
 * This structure is described here at https://en.wikipedia.org/wiki/B%2B_tree
 * <p>
 * Each node can hold either some innerNodes along with keyParts which are pointer nodes to additional pointer or
 * finally leafNodes which contain actual values.
 * The keyParts dictate which innerNode should be followed when looking up a specific key done via log(N).
 * A leaf node contains the actual value for the given key.
 * <p>
 * An IndexNode cannot have both innerNodes and leafNodes
 * <p>
 * Each  index node is linked to a specific SoftIndexFileStore segment that is not the same thing as a cache segment.
 * Whenever a cache segment is used its variable will be named cacheSegment or something similar to help prevent
 * ambiguity.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class IndexNode {
   private static final Log log = LogFactory.getLog(IndexNode.class, Log.class);

   private static final byte HAS_LEAVES = 1;
   private static final byte HAS_NODES = 2;
   private static final int INNER_NODE_HEADER_SIZE = 5;
   private static final int INNER_NODE_REFERENCE_SIZE = 10;
   private static final int LEAF_NODE_REFERENCE_SIZE = 14;

   public static final int RESERVED_SPACE
         = INNER_NODE_HEADER_SIZE + 2 * Math.max(INNER_NODE_REFERENCE_SIZE, LEAF_NODE_REFERENCE_SIZE);

   private final Index.Segment segment;
   private byte[] prefix;
   private byte[][] keyParts;
   private InnerNode[] innerNodes;
   private LeafNode[] leafNodes = LeafNode.EMPTY_ARRAY;
   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private long offset = -1;
   private short contentLength = -1;
   private short totalLength = -1;
   private short occupiedSpace;

   public enum RecordChange {
      INCREASE,
      INCREASE_FOR_OLD,
      MOVE,
      DECREASE,
   }

   IndexNode(Index.Segment segment, long offset, short occupiedSpace) throws IOException {
      this.segment = segment;
      this.offset = offset;
      this.occupiedSpace = occupiedSpace;

      ByteBuffer buffer = loadBuffer(segment.getIndexFile(), offset, occupiedSpace);

      prefix = new byte[buffer.getShort()];
      buffer.get(prefix);

      byte flags = buffer.get();
      int numKeyParts = buffer.getShort();

      keyParts = new byte[numKeyParts][];
      for (int i = 0; i < numKeyParts; ++i) {
         keyParts[i] = new byte[buffer.getShort()];
         buffer.get(keyParts[i]);
      }

      if ((flags & HAS_LEAVES) != 0) {
         leafNodes = new LeafNode[numKeyParts + 1];
         for (int i = 0; i < numKeyParts + 1; ++i) {
            leafNodes[i] = new LeafNode(buffer.getInt(), buffer.getInt(), buffer.getShort(), buffer.getInt());
         }
      } else if ((flags & HAS_NODES) != 0){
         innerNodes = new InnerNode[numKeyParts + 1];
         for (int i = 0; i < numKeyParts + 1; ++i) {
            innerNodes[i] = new InnerNode(buffer.getLong(), buffer.getShort());
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("Loaded %08x from %d:%d (length %d)", System.identityHashCode(this), offset, occupiedSpace, length());
      }
   }

   private static ByteBuffer loadBuffer(FileChannel indexFile, long offset, int occupiedSpace) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(occupiedSpace);
      int read = 0;
      do {
         int nowRead = indexFile.read(buffer, offset + read);
         if (nowRead < 0) {
            throw new IOException("Cannot read record [" + offset + ":" + occupiedSpace + "] (already read "
                  + read + "), file size is " + indexFile.size());
         }
         read += nowRead;
      } while (read < occupiedSpace);
      buffer.rewind();
      return buffer;
   }

   private IndexNode(Index.Segment segment, byte[] newPrefix, byte[][] newKeyParts, LeafNode[] newLeafNodes) {
      this.segment = segment;
      this.prefix = newPrefix;
      this.keyParts = newKeyParts;
      this.leafNodes = newLeafNodes;
   }

   private IndexNode(Index.Segment segment, byte[] newPrefix, byte[][] newKeyParts, InnerNode[] newInnerNodes) {
      this.segment = segment;
      this.prefix = newPrefix;
      this.keyParts = newKeyParts;
      this.innerNodes = newInnerNodes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IndexNode indexNode = (IndexNode) o;

      if (!Arrays.equals(innerNodes, indexNode.innerNodes)) return false;
      if (!Arrays.equals(leafNodes, indexNode.leafNodes)) return false;
      if (!Arrays.equals(prefix, indexNode.prefix)) return false;
      if (!Arrays.deepEquals(keyParts, indexNode.keyParts)) return false;

      return true;
   }

   /**
    * Can be called only from single writer thread (therefore the write lock guards only other readers)
    */
   private void replaceContent(IndexNode other) throws IOException {
      try {
         lock.writeLock().lock();
         this.prefix = other.prefix;
         this.keyParts = other.keyParts;
         this.innerNodes = other.innerNodes;
         this.leafNodes = other.leafNodes;
         this.contentLength = -1;
         this.totalLength = -1;
      } finally {
         lock.writeLock().unlock();
      }

      // don't have to acquire any lock here
      // the only node with offset < 0 is the root - we can't lose reference to it
      if (offset >= 0) {
         store(new Index.IndexSpace(offset, occupiedSpace));
      }
   }

   // called only internally or for root
   void store(Index.IndexSpace indexSpace) throws IOException {
      this.offset = indexSpace.offset;
      this.occupiedSpace = indexSpace.length;
      ByteBuffer buffer = ByteBuffer.allocate(length());
      buffer.putShort((short) prefix.length);
      buffer.put(prefix);
      byte flags = 0;
      if (innerNodes != null && innerNodes.length != 0) {
         flags |= HAS_NODES;
      } else if (leafNodes != null && leafNodes.length != 0) {
         flags |= HAS_LEAVES;
      }
      buffer.put(flags);
      buffer.putShort((short) keyParts.length);
      for (byte[] keyPart : keyParts) {
         buffer.putShort((short) keyPart.length);
         buffer.put(keyPart);
      }
      if (innerNodes != null) {
         for (InnerNode innerNode : innerNodes) {
            buffer.putLong(innerNode.offset);
            buffer.putShort(innerNode.length);
         }
      } else {
         for (LeafNode leafNode : leafNodes) {
            buffer.putInt(leafNode.file);
            buffer.putInt(leafNode.offset);
            buffer.putShort(leafNode.numRecords);
            buffer.putInt(leafNode.cacheSegment);
         }
      }
      assert buffer.position() == buffer.limit() : "Buffer position: " + buffer.position() + " limit: " + buffer.limit();
      buffer.flip();
      segment.getIndexFile().write(buffer, offset);

      if (log.isTraceEnabled()) {
         log.tracef("Persisted %08x (length %d, %d %s) to %d:%d", System.identityHashCode(this), length(),
            innerNodes != null ? innerNodes.length : leafNodes.length,
            innerNodes != null ? "children" : "leaves", offset, occupiedSpace);
      }
   }

   private static class Path {
      IndexNode node;
      public int index;

      private Path(IndexNode node, int index) {
         this.node = node;
         this.index = index;
      }
   }

   private static boolean entryKeyEqualsBuffer(EntryRecord headerAndKey, org.infinispan.commons.io.ByteBuffer buffer) {
      byte[] key = headerAndKey.getKey();
      return Util.arraysEqual(key, 0, key.length, buffer.getBuf(), buffer.getOffset(), buffer.getOffset() + buffer.getLength());
   }

   public enum ReadOperation {
      GET_RECORD {
         @Override
         protected EntryRecord apply(LeafNode leafNode, org.infinispan.commons.io.ByteBuffer key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            return leafNode.loadRecord(fileProvider, key, timeService);
         }
      },
      GET_EXPIRED_RECORD {
         @Override
         protected EntryRecord apply(LeafNode leafNode, org.infinispan.commons.io.ByteBuffer key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            return leafNode.loadRecord(fileProvider, key, null);
         }
      },
      GET_POSITION {
         @Override
         protected EntryPosition apply(LeafNode leafNode, org.infinispan.commons.io.ByteBuffer key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            EntryRecord hak = leafNode.loadHeaderAndKey(fileProvider);
            if (entryKeyEqualsBuffer(hak, key)) {
               if (hak.getHeader().expiryTime() > 0 && hak.getHeader().expiryTime() <= timeService.wallClockTime()) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Found node on %d:%d but it is expired", leafNode.file, leafNode.offset);
                  }
                  return null;
               }
               return leafNode;
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("Found node on %d:%d but key does not match", leafNode.file, leafNode.offset);
               }
            }
            return null;
         }
      },
      GET_INFO {
         @Override
         protected EntryInfo apply(LeafNode leafNode, org.infinispan.commons.io.ByteBuffer key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            EntryRecord hak = leafNode.loadHeaderAndKey(fileProvider);
            if (entryKeyEqualsBuffer(hak, key)) {
               return leafNode;
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("Found node on %d:%d but key does not match", leafNode.file, leafNode.offset);
               }
               return null;
            }
         }
      };

      protected abstract <T> T apply(LeafNode leafNode, org.infinispan.commons.io.ByteBuffer key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException;
   }

   public static <T> T applyOnLeaf(Index.Segment segment, int cacheSegment, byte[] indexKey, Lock rootLock, ReadOperation operation) throws IOException {
      int attempts = 0;
      for (; ; ) {
         rootLock.lock();
         IndexNode node = segment.getRoot();
         Lock parentLock = rootLock, currentLock = null;
         try {
            while (node.innerNodes != null) {
               currentLock = node.lock.readLock();
               currentLock.lock();
               parentLock.unlock();
               parentLock = currentLock;
               int insertionPoint = node.getInsertionPoint(indexKey);
               node = node.innerNodes[insertionPoint].getIndexNode(segment);
               if (node == null) {
                  return null;
               }
            }
            currentLock = node.lock.readLock();
            currentLock.lock();
            if (node.leafNodes.length == 0) {
               return null;
            }
            int insertionPoint = node.getInsertionPoint(indexKey);
            int cacheSegmentBytesSize = UnsignedNumeric.sizeUnsignedInt(cacheSegment);
            return operation.apply(node.leafNodes[insertionPoint], ByteBufferImpl.create(indexKey, cacheSegmentBytesSize, indexKey.length - cacheSegmentBytesSize),
                  segment.getFileProvider(), segment.getTimeService());
         } catch (IndexNodeOutdatedException e) {
            try {
               if (attempts > 10) {
                  throw log.indexLooksCorrupt(e);
               }
               Thread.sleep(1000);
               attempts++;
            } catch (InterruptedException e1) {
               Thread.currentThread().interrupt();
            }
            // noop, we'll simply retry
         } finally {
            if (parentLock != currentLock) parentLock.unlock();
            if (currentLock != null) currentLock.unlock();
         }
      }
   }

   public static long calculateMaxSeqId(Index.Segment segment, Lock lock) throws IOException {
      lock.lock();
      try {
         return calculateMaxSeqId(segment.getRoot(), segment);
      } finally {
         lock.unlock();
      }
   }

   private static long calculateMaxSeqId(IndexNode node, Index.Segment segment) throws IOException {
      long maxSeqId = 0;
      node.lock.readLock().lock();
      try {
         if (node.leafNodes != null) {
            for (LeafNode ln : node.leafNodes) {
               EntryRecord record = ln.loadHeaderAndKey(segment.getFileProvider());
               maxSeqId = Math.max(maxSeqId, record.getHeader().seqId());
            }
         }
         if (node.innerNodes != null) {
            for (InnerNode in : node.innerNodes) {
               maxSeqId = Math.max(maxSeqId, calculateMaxSeqId(in.getIndexNode(segment), segment));
            }
         }
      } catch (IndexNodeOutdatedException e) {
         throw log.indexLooksCorrupt(e);
      } finally {
         node.lock.readLock().unlock();
      }
      return maxSeqId;
   }

   private void updateFileOffsetInFile(int leafOffset, int newFile, int newOffset, short numRecords) throws IOException {
      // Root is -1, so that means the beginning of the file
      long offset = this.offset >= 0 ? this.offset : 0;
      offset += headerLength();
      for (byte[] keyPart : this.keyParts) {
         offset += 2 + keyPart.length;
      }
      offset += (long) leafOffset * LEAF_NODE_REFERENCE_SIZE;

      ByteBuffer buffer = ByteBuffer.allocate(10);
      buffer.putInt(newFile);
      buffer.putInt(newOffset);
      buffer.putShort(numRecords);

      buffer.flip();

      this.segment.getIndexFile().write(buffer, offset);
   }

   private static IndexNode findParentNode(IndexNode root, byte[] indexKey, Deque<Path> stack) throws IOException {
      IndexNode node = root;
      while (node.innerNodes != null) {
         int insertionPoint = node.getInsertionPoint(indexKey);
         if (stack != null) stack.push(new Path(node, insertionPoint));
         if (log.isTraceEnabled()) {
            log.tracef("Pushed %08x (length %d, %d children) to stack (insertion point %d)", System.identityHashCode(node), node.length(), node.innerNodes.length, insertionPoint);
         }
         node = node.innerNodes[insertionPoint].getIndexNode(root.segment);
      }
      return node;
   }

   public static void setPosition(IndexNode root, int cacheSegment, Object objectKey, org.infinispan.commons.io.ByteBuffer key, int file, int offset, int size, OverwriteHook overwriteHook, RecordChange recordChange) throws IOException {
      setPosition(root, cacheSegment, objectKey, Index.toIndexKey(cacheSegment, key), file, offset, size, overwriteHook, recordChange);
   }

   private static void setPosition(IndexNode root, int cacheSegment, Object objectKey, byte[] indexKey, int file, int offset, int size, OverwriteHook overwriteHook, RecordChange recordChange) throws IOException {
      Deque<Path> stack = new ArrayDeque<>();
      IndexNode node = findParentNode(root, indexKey, stack);
      IndexNode copy = node.copyWith(cacheSegment, objectKey, indexKey, file, offset, size, overwriteHook, recordChange);
      if (copy == node) {
         // no change was executed
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Created %08x (length %d) from %08x (length %d), stack size %d",
               System.identityHashCode(copy), copy.length(), System.identityHashCode(node), node.length(), stack.size());
      }

      Deque<IndexNode> garbage = new ArrayDeque<>();
      try {
         JoinSplitResult result = manageLength(root.segment, stack, node, copy, garbage);
         if (result == null) {
            return;
         }
         if (log.isTraceEnabled()) {
            log.tracef("Created (1) %d new nodes, GC %08x", result.newNodes.size(), System.identityHashCode(node));
         }
         garbage.push(node);
         for (;;) {
            if (stack.isEmpty()) {
               IndexNode newRoot;
               if (result.newNodes.size() == 1) {
                  newRoot = result.newNodes.get(0);
                  if (log.isTraceEnabled()) {
                     log.tracef("Setting new root %08x (index has shrunk)", System.identityHashCode(newRoot));
                  }
               } else {
                  newRoot = IndexNode.emptyWithInnerNodes(root.segment).copyWith(0, 0, result.newNodes);
                  root.segment.getIndexFile().force(false);
                  if (log.isTraceEnabled()) {
                     log.tracef("Setting new root %08x (index has grown)", System.identityHashCode(newRoot));
                  }
               }
               newRoot.segment.setRoot(newRoot);
               return;
            }
            Path path = stack.pop();
            copy = path.node.copyWith(result.from, result.to, result.newNodes);
            if (log.isTraceEnabled()) {
               log.tracef("Created %08x (length %d) from %08x with the %d new nodes (%d - %d)",
                     System.identityHashCode(copy), copy.length(), System.identityHashCode(path.node), result.newNodes.size(), result.from, result.to);
            }
            result = manageLength(path.node.segment, stack, path.node, copy, garbage);
            if (result == null) {
               if (log.isTraceEnabled()) {
                  log.tracef("No more index updates required");
               }
               return;
            }
            if (log.isTraceEnabled()) {
               log.tracef("Created (2) %d new nodes, GC %08x", result.newNodes.size(), System.identityHashCode(path.node));
            }
            garbage.push(path.node);
         }
      } finally {
         while (!garbage.isEmpty()) {
            IndexNode oldNode = garbage.pop();
            oldNode.lock.writeLock().lock();
            try {
               if (oldNode.offset >= 0) {
                  oldNode.segment.freeIndexSpace(oldNode.offset, oldNode.occupiedSpace);
                  oldNode.offset = -1;
                  oldNode.occupiedSpace = -1;
               }
            } finally {
               oldNode.lock.writeLock().unlock();
            }
         }
      }
   }

   private static class JoinSplitResult {
      public final int from;
      public final int to;
      final List<IndexNode> newNodes;

      private JoinSplitResult(int from, int to, List<IndexNode> newNodes) {
         this.from = from;
         this.to = to;
         this.newNodes = newNodes;
      }
   }

   private static JoinSplitResult manageLength(Index.Segment segment, Deque<Path> stack, IndexNode node, IndexNode copy, Deque<IndexNode> garbage) throws IOException {
      int from, to;
      if (copy.length() < segment.getMinNodeSize() && !stack.isEmpty()) {
         Path parent = stack.peek();
         if (parent.node.innerNodes.length == 1) {
            // we have no siblings - we can't merge with them even when we're really short
            if (copy.length() <= node.occupiedSpace) {
               node.replaceContent(copy);
               return null;
            } else {
               return new JoinSplitResult(parent.index, parent.index, Collections.singletonList(copy));
            }
         }
         int sizeWithLeft = Integer.MAX_VALUE;
         int sizeWithRight = Integer.MAX_VALUE;
         if (parent.index > 0) {
            sizeWithLeft = copy.length() + parent.node.innerNodes[parent.index - 1].length - INNER_NODE_HEADER_SIZE;
         }
         if (parent.index < parent.node.innerNodes.length - 1) {
            sizeWithRight = copy.length() + parent.node.innerNodes[parent.index + 1].length - INNER_NODE_HEADER_SIZE;
         }
         int joinWith;
         // this is just some kind of heuristic, may be changed later
         if (sizeWithLeft == Integer.MAX_VALUE) {
            joinWith = parent.index + 1;
         } else if (sizeWithRight == Integer.MAX_VALUE) {
            joinWith = parent.index - 1;
         } else if (sizeWithLeft > segment.getMaxNodeSize() && sizeWithRight > segment.getMaxNodeSize()) {
            joinWith = sizeWithLeft >= sizeWithRight ? parent.index - 1 : parent.index + 1;
         } else {
            joinWith = sizeWithLeft <= sizeWithRight ? parent.index - 1 : parent.index + 1;
         }
         if (joinWith < 0 || joinWith >= parent.node.innerNodes.length) {
            throw new IllegalStateException(String.format("parent %08x, %08x -> %08x: cannot join to %d, with left %d, with right %d, max %d",
                  System.identityHashCode(parent.node), System.identityHashCode(node), System.identityHashCode(copy),
                  joinWith, sizeWithLeft, sizeWithRight, segment.getMaxNodeSize()));
         }
         IndexNode joiner = parent.node.innerNodes[joinWith].getIndexNode(segment);
         byte[] middleKey = concat(parent.node.prefix, parent.node.keyParts[joinWith < parent.index ? parent.index - 1 : parent.index]);
         if (joinWith < parent.index) {
            copy = join(joiner, middleKey, copy);
            from = joinWith;
            to = parent.index;
         } else {
            copy = join(copy, middleKey, joiner);
            from = parent.index;
            to = joinWith;
         }
         garbage.push(joiner);
      } else if (copy.length() <= node.occupiedSpace) {
         if (copy.innerNodes != null && copy.innerNodes.length == 1 && stack.isEmpty()) {
            IndexNode child = copy.innerNodes[0].getIndexNode(copy.segment);
            return new JoinSplitResult(0, 0, Collections.singletonList(child));
         } else {
            // special case where we only overwrite the key
            node.replaceContent(copy);
            return null;
         }
      } else if (stack.isEmpty()) {
         from = to = 0;
      } else {
         from = to = stack.peek().index;
      }
      if (copy.length() <= segment.getMaxNodeSize()) {
         return new JoinSplitResult(from, to, Collections.singletonList(copy));
      } else {
         return new JoinSplitResult(from, to, copy.split());
      }
   }

   private static IndexNode join(IndexNode left, byte[] middleKey, IndexNode right) throws IOException {
      byte[] newPrefix = commonPrefix(left.prefix, right.prefix);
      byte[][] newKeyParts = new byte[left.keyParts.length + right.keyParts.length + 1][];
      newPrefix = commonPrefix(newPrefix, middleKey);
      copyKeyParts(left.keyParts, 0, newKeyParts, 0, left.keyParts.length, left.prefix, newPrefix);
      byte[] rightmostKey;
      try {
         rightmostKey = left.rightmostKey();
      } catch (IndexNodeOutdatedException e) {
         throw new IllegalStateException(e);
      }
      int commonLength = Math.abs(compare(middleKey, rightmostKey));
      newKeyParts[left.keyParts.length] = substring(middleKey, newPrefix.length, commonLength);
      copyKeyParts(right.keyParts, 0, newKeyParts, left.keyParts.length + 1, right.keyParts.length, right.prefix, newPrefix);
      if (left.innerNodes != null && right.innerNodes != null) {
         InnerNode[] newInnerNodes = new InnerNode[left.innerNodes.length + right.innerNodes.length];
         System.arraycopy(left.innerNodes, 0, newInnerNodes, 0, left.innerNodes.length);
         System.arraycopy(right.innerNodes, 0, newInnerNodes, left.innerNodes.length, right.innerNodes.length);
         return new IndexNode(left.segment, newPrefix, newKeyParts, newInnerNodes);
      } else if (left.leafNodes != null && right.leafNodes != null) {
         LeafNode[] newLeafNodes = new LeafNode[left.leafNodes.length + right.leafNodes.length];
         System.arraycopy(left.leafNodes, 0, newLeafNodes, 0, left.leafNodes.length);
         System.arraycopy(right.leafNodes, 0, newLeafNodes, left.leafNodes.length, right.leafNodes.length);
         return new IndexNode(left.segment, newPrefix, newKeyParts, newLeafNodes);
      } else {
         throw new IllegalArgumentException("Cannot join " + left + " and " + right);
      }
   }

   private IndexNode copyWith(int oldNodesFrom, int oldNodesTo, List<IndexNode> newNodes) throws IOException {
      InnerNode[] newInnerNodes = new InnerNode[innerNodes.length + newNodes.size() - 1 - oldNodesTo + oldNodesFrom];
      System.arraycopy(innerNodes, 0, newInnerNodes, 0, oldNodesFrom);
      System.arraycopy(innerNodes, oldNodesTo + 1, newInnerNodes, oldNodesFrom + newNodes.size(), innerNodes.length - oldNodesTo - 1);
      for (int i = 0; i < newNodes.size(); ++i) {
         IndexNode node = newNodes.get(i);
         Index.IndexSpace space = segment.allocateIndexSpace(node.length());
         node.store(space);
         newInnerNodes[i + oldNodesFrom] = new InnerNode(node);
      }
      byte[][] newKeys = new byte[newNodes.size() - 1][];
      byte[] newPrefix = prefix;
      for (int i = 0; i < newKeys.length; ++i) {
         try {
            // TODO: if all keys within the subtree are null (deleted), the new key will be null
            // will be fixed with proper index reduction
            newKeys[i] = newNodes.get(i + 1).leftmostKey();
            if (newKeys[i] == null) {
               throw new IllegalStateException();
            }
         } catch (IndexNodeOutdatedException e) {
            throw new IllegalStateException("Index cannot be outdated for segment updater thread", e);
         }
         newPrefix = commonPrefix(newPrefix, newKeys[i]);
      }
      byte[][] newKeyParts = new byte[keyParts.length + newNodes.size() - 1 - oldNodesTo + oldNodesFrom][];
      copyKeyParts(keyParts, 0, newKeyParts, 0, oldNodesFrom, prefix, newPrefix);
      copyKeyParts(keyParts, oldNodesTo, newKeyParts, oldNodesFrom + newKeys.length, keyParts.length - oldNodesTo, prefix, newPrefix);
      for (int i = 0; i < newKeys.length; ++i) {
         newKeyParts[i + oldNodesFrom] = substring(newKeys[i], newPrefix.length, newKeys[i].length);
      }
      return new IndexNode(segment, newPrefix, newKeyParts, newInnerNodes);
   }

   private byte[] leftmostKey() throws IOException, IndexNodeOutdatedException {
      if (innerNodes != null) {
         for (InnerNode innerNode : innerNodes) {
            byte[] key = innerNode.getIndexNode(segment).leftmostKey();
            if (key != null) return key;
         }
      } else {
         for (LeafNode leafNode : leafNodes) {
            EntryRecord hak = leafNode.loadHeaderAndKey(segment.getFileProvider());
            if (hak.getKey() != null) return Index.toIndexKey(leafNode.cacheSegment, hak.getKey());
         }
      }
      return null;
   }

   private byte[] rightmostKey() throws IOException, IndexNodeOutdatedException {
      if (innerNodes != null) {
         for (int i = innerNodes.length - 1; i >= 0; --i) {
            byte[] key = innerNodes[i].getIndexNode(segment).rightmostKey();
            if (key != null) return key;
         }
      } else {
         for (int i = leafNodes.length - 1; i >= 0; --i) {
            EntryRecord hak = leafNodes[i].loadHeaderAndKey(segment.getFileProvider());
            if (hak.getKey() != null) return Index.toIndexKey(leafNodes[i].cacheSegment, hak.getKey());
         }
      }
      return null;
   }

   /**
    * Called on the most bottom node
    */
   private IndexNode copyWith(int cacheSegment, Object objectKey, byte[] indexKey, int file, int offset, int size, OverwriteHook overwriteHook, RecordChange recordChange) throws IOException {
      if (leafNodes == null) throw new IllegalArgumentException();
      byte[] newPrefix;
      if (leafNodes.length == 0) {
         overwriteHook.setOverwritten(cacheSegment, false, -1, -1);
         if (overwriteHook.check(-1, -1)) {
            return new IndexNode(segment, prefix, keyParts, new LeafNode[]{new LeafNode(file, offset, (short) 1, cacheSegment)});
         } else {
            segment.getCompactor().free(file, size);
            return this;
         }
      }

      int insertPart = getInsertionPoint(indexKey);
      LeafNode oldLeafNode = leafNodes[insertPart];

      short numRecords = oldLeafNode.numRecords;
      switch (recordChange) {
         case INCREASE:
         case INCREASE_FOR_OLD:
            if (numRecords == Short.MAX_VALUE) {
               throw new IllegalStateException("Too many records for this key (short overflow)");
            }
            numRecords++;
            break;
         case MOVE:
            break;
         case DECREASE:
            numRecords--;
            break;
      }

      byte[][] newKeyParts;
      LeafNode[] newLeafNodes;
      EntryRecord hak;
      try {
         hak = oldLeafNode.loadHeaderAndKey(segment.getFileProvider());
      } catch (IndexNodeOutdatedException e) {
         throw new IllegalStateException("Index cannot be outdated for segment updater thread", e);
      }
      byte[] oldIndexKey = Index.toIndexKey(oldLeafNode.cacheSegment, hak.getKey());
      int keyComp = compare(oldIndexKey, indexKey);
      if (keyComp == 0) {
         if (numRecords > 0) {
            if (overwriteHook.check(oldLeafNode.file, oldLeafNode.offset)) {
               if (recordChange == RecordChange.INCREASE || recordChange == RecordChange.MOVE) {
                  if (log.isTraceEnabled()) {
                     log.trace(String.format("Overwriting %s %d:%d with %d:%d (%d)", objectKey,
                           oldLeafNode.file, oldLeafNode.offset, file, offset, numRecords));
                  }

                  updateFileOffsetInFile(insertPart, file, offset, numRecords);

                  segment.getCompactor().free(oldLeafNode.file, hak.getHeader().totalLength());
               } else {
                  if (log.isTraceEnabled()) {
                     log.trace(String.format("Updating num records for %s %d:%d to %d", objectKey, oldLeafNode.file, oldLeafNode.offset, numRecords));
                  }
                  if (recordChange == RecordChange.INCREASE_FOR_OLD) {
                     // Mark old files as freed for compactor when rebuilding index
                     segment.getCompactor().free(file, size);
                  }
                  // We don't need to update the file as the file and position are the same, only the numRecords
                  // has been updated for REMOVED
                  file = oldLeafNode.file;
                  offset = oldLeafNode.offset;
               }
               lock.writeLock().lock();
               try {
                  leafNodes[insertPart] = new LeafNode(file, offset, numRecords, cacheSegment);
               } finally {
                  lock.writeLock().unlock();
               }

               overwriteHook.setOverwritten(cacheSegment, true, oldLeafNode.file, oldLeafNode.offset);
               return this;
            } else {
               overwriteHook.setOverwritten(cacheSegment, false, -1, -1);
               segment.getCompactor().free(file, size);
               return this;
            }
         } else {
            overwriteHook.setOverwritten(cacheSegment, true, oldLeafNode.file, oldLeafNode.offset);
            if (keyParts.length <= 1) {
               newPrefix = Util.EMPTY_BYTE_ARRAY;
               newKeyParts = Util.EMPTY_BYTE_ARRAY_ARRAY;
            } else {
               newPrefix = prefix;
               newKeyParts = new byte[keyParts.length - 1][];
               if (insertPart == keyParts.length) {
                  System.arraycopy(keyParts, 0, newKeyParts, 0, newKeyParts.length);
               } else {
                  System.arraycopy(keyParts, 0, newKeyParts, 0, insertPart);
                  System.arraycopy(keyParts, insertPart + 1, newKeyParts, insertPart, newKeyParts.length - insertPart);
               }
            }
            if (leafNodes.length > 0) {
               newLeafNodes = new LeafNode[leafNodes.length - 1];
               System.arraycopy(leafNodes, 0, newLeafNodes, 0, insertPart);
               System.arraycopy(leafNodes, insertPart + 1, newLeafNodes, insertPart, newLeafNodes.length - insertPart);
            } else {
               newLeafNodes = leafNodes;
            }
            segment.getCompactor().free(oldLeafNode.file, hak.getHeader().totalLength());
         }
      } else {
         // IndexRequest cannot be MOVED or DROPPED when the key is not in the index
         assert recordChange == RecordChange.INCREASE;
         overwriteHook.setOverwritten(cacheSegment, false, -1, -1);

         // We have to insert the record even if this is a delete request and the key was not found
         // because otherwise we would have incorrect numRecord count. Eventually, Compactor will
         // drop the tombstone and update index, removing this node
         if (keyParts.length == 0) {
            // TODO: we may use unnecessarily long keys here and the key is never shortened
            newPrefix = keyComp > 0 ? indexKey : oldIndexKey;
         } else {
            newPrefix = commonPrefix(prefix, indexKey);
         }
         newKeyParts = new byte[keyParts.length + 1][];
         newLeafNodes = new LeafNode[leafNodes.length + 1];
         copyKeyParts(keyParts, 0, newKeyParts, 0, insertPart, prefix, newPrefix);
         copyKeyParts(keyParts, insertPart, newKeyParts, insertPart + 1, keyParts.length - insertPart, prefix, newPrefix);
         if (keyComp > 0) {
            newKeyParts[insertPart] = substring(indexKey, newPrefix.length, keyComp);
            System.arraycopy(leafNodes, 0, newLeafNodes, 0, insertPart + 1);
            System.arraycopy(leafNodes, insertPart + 1, newLeafNodes, insertPart + 2, leafNodes.length - insertPart - 1);
            log.tracef("Creating new leafNode for %s at %d:%d", objectKey, file, offset);
            newLeafNodes[insertPart + 1] = new LeafNode(file, offset, (short) 1, cacheSegment);
         } else {
            newKeyParts[insertPart] = substring(oldIndexKey, newPrefix.length, -keyComp);
            System.arraycopy(leafNodes, 0, newLeafNodes, 0, insertPart);
            System.arraycopy(leafNodes, insertPart, newLeafNodes, insertPart + 1, leafNodes.length - insertPart);
            log.tracef("Creating new leafNode for %s at %d:%d", objectKey, file, offset);
            newLeafNodes[insertPart] = new LeafNode(file, offset, (short) 1, cacheSegment);
         }
      }
      return new IndexNode(segment, newPrefix, newKeyParts, newLeafNodes);
   }

   private int getIterationPoint(byte[] key, int cacheSegment) {
      int comp = compare(key, prefix, prefix.length);
      int insertionPoint;
      if (comp > 0) {
         insertionPoint = 0;
      } else if (comp < 0) {
         insertionPoint = keyParts.length;
      } else {
         byte[] keyPostfix = substring(key, prefix.length, key.length);
         insertionPoint = Arrays.binarySearch(keyParts, keyPostfix, (o1, o2) -> IndexNode.compare(o2, o1));
         if (insertionPoint < 0) {
            insertionPoint = -insertionPoint - 1;
         } else {
            int cacheSegmentToUse = cacheSegment < 0 ? UnsignedNumeric.readUnsignedInt(key, 0) : cacheSegment;
            if (UnsignedNumeric.sizeUnsignedInt(cacheSegmentToUse) < key.length) {
               // When the length is bigger than a cache segment, that means the index prefix is a specific key and if it
               // is equal we have to skip two spaces
               // Example:
               // KeyParts
               // 84 = {byte[12]@9221} [-100, 1, -104, 1, 2, -118, 1, 5, 10, 3, 40, -71]
               // 85 = {byte[12]@9222} [-100, 1, -104, 1, 2, -118, 1, 5, 10, 3, 40, -60]
               // 86 = {byte[13]@9223} [-100, 1, -104, 1, 2, -118, 1, 5, 10, 3, 40, -60, 14]
               // 87 = {byte[12]@9224} [-100, 1, -104, 1, 2, -118, 1, 5, 10, 3, 40, -54]
               // 88 = {byte[12]@9225} [-100, 1, -104, 1, 2, -118, 1, 5, 10, 3, 40, -48]
               // Segment Prefix
               //     {byte[13]        [-100, 1, -104, 1, 2, -118, 1, 5, 10, 3, 40, -60, 14]
               // The actual value is stored at 87 in this case per `getInsertionPoint` so we need to skip to 88
               // CacheSegment is -1 for an innerNode because we have to find where in the leaf node the value is
               // CacheSegment is > 0 for a leafNode
               insertionPoint += cacheSegment < 0 ? 1 : 2;
            }
         }
      }
      return insertionPoint;
   }

   private int getInsertionPoint(byte[] key) {
      int comp = compare(key, prefix, prefix.length);
      int insertionPoint;
      if (comp > 0) {
         insertionPoint = 0;
      } else if (comp < 0) {
         insertionPoint = keyParts.length;
      } else {
         byte[] keyPostfix = substring(key, prefix.length, key.length);
         insertionPoint = Arrays.binarySearch(keyParts, keyPostfix, (o1, o2) -> IndexNode.compare(o2, o1));
         if (insertionPoint < 0) {
            insertionPoint = -insertionPoint - 1;
         } else {
            insertionPoint++; // identical elements must go to the right
         }
      }

      return insertionPoint;
   }

   private List<IndexNode> split() {
      int headerLength = headerLength();
      int contentLength = contentLength();
      int maxLength = segment.getMaxNodeSize();
      int targetParts = contentLength / Math.max(maxLength - headerLength, 1) + 1;
      int targetLength = contentLength / targetParts + headerLength;
      List<IndexNode> list = new ArrayList<>();
      int childLength = innerNodes != null ? INNER_NODE_REFERENCE_SIZE : LEAF_NODE_REFERENCE_SIZE;
      byte[] prefixExtension = keyParts[0]; // the prefix can be only extended
      int currentLength = INNER_NODE_HEADER_SIZE + prefix.length + prefixExtension.length + 2 * childLength + 2;
      int nodeFrom = 0;
      // TODO: under certain circumstances this algorithm can end up by splitting node into very uneven parts
      // such as having one part with only 1 child, therefore only 15 bytes long
      for (int i = 1; i < keyParts.length; ++i) {
         int newLength;
         byte[] newPrefixExtension = commonPrefix(prefixExtension, keyParts[i]);
         if (newPrefixExtension.length != prefixExtension.length) {
            newLength = currentLength + (prefixExtension.length - newPrefixExtension.length) * (i - nodeFrom - 1);
         } else {
            newLength = currentLength;
         }
         newLength += keyParts[i].length - newPrefixExtension.length + childLength + 2;
         if (newLength < targetLength) {
            currentLength = newLength;
         } else {
            IndexNode subNode;
            if (newLength > maxLength) {
               subNode = subNode(prefixExtension, nodeFrom, i);
               ++i;
            } else {
               subNode = subNode(newPrefixExtension, nodeFrom, i + 1);
               i += 2;
            }
            list.add(subNode);
            if (i < keyParts.length) {
               newPrefixExtension = keyParts[i];
            }
            currentLength = INNER_NODE_HEADER_SIZE + prefix.length + newPrefixExtension.length + 2 * childLength + 2;
            nodeFrom = i;
         }
         prefixExtension = newPrefixExtension;
      }
      if (nodeFrom <= keyParts.length) {
         list.add(subNode(prefixExtension, nodeFrom, keyParts.length));
      }
      return list;
   }

   private IndexNode subNode(byte[] newPrefixExtension, int childFrom, int childTo) {
      // first node takes up to child[to + 1], other do not take the child[from] == child[previousTo + 1]
      // If the new node has > 1 keyParts, it ignores the first keyPart, otherwise it just sets the first child to be
      // deleted (empty entry)
      byte[][] newKeyParts = new byte[childTo - childFrom][];
      if (newPrefixExtension.length > 0) {
         for (int i = childFrom; i < childTo; ++i) {
            newKeyParts[i - childFrom] = substring(keyParts[i], newPrefixExtension.length, keyParts[i].length);
         }
      } else {
         System.arraycopy(keyParts, childFrom, newKeyParts, 0, childTo - childFrom);
      }
      byte[] newPrefix = childFrom == childTo ? Util.EMPTY_BYTE_ARRAY : concat(prefix, newPrefixExtension);
      if (innerNodes != null) {
         InnerNode[] newInnerNodes = new InnerNode[childTo - childFrom + 1];
         System.arraycopy(innerNodes, childFrom, newInnerNodes, 0, childTo - childFrom + 1);
         return new IndexNode(segment, newPrefix, newKeyParts, newInnerNodes);
      } else if (leafNodes != null) {
         LeafNode[] newLeafNodes = new LeafNode[childTo - childFrom + 1];
         System.arraycopy(leafNodes, childFrom, newLeafNodes, 0, childTo - childFrom + 1);
         return new IndexNode(segment, newPrefix, newKeyParts, newLeafNodes);
      }
      throw new IllegalStateException();
   }

   private static byte[] concat(byte[] first, byte[] second) {
      if (first == null || first.length == 0) return second;
      if (second == null || second.length == 0) return first;
      byte[] result = new byte[first.length + second.length];
      System.arraycopy(first, 0, result, 0, first.length);
      System.arraycopy(second, 0, result, first.length, second.length);
      return result;
   }

   private static void copyKeyParts(byte[][] src, int srcIndex, byte[][] dest, int destIndex, int length, byte[] oldPrefix, byte[] common) {
      if (oldPrefix.length == common.length) {
         System.arraycopy(src, srcIndex, dest, destIndex, length);
      } else {
         for (int i = 0; i < length; ++i) {
            dest[destIndex + i] = findNewKeyPart(oldPrefix, src[srcIndex + i], common);
         }
      }
   }

   private static byte[] findNewKeyPart(byte[] oldPrefix, byte[] oldKeyPart, byte[] common) {
      byte[] newPart = new byte[oldKeyPart.length + oldPrefix.length - common.length];
      System.arraycopy(oldPrefix, common.length, newPart, 0, oldPrefix.length - common.length);
      System.arraycopy(oldKeyPart, 0, newPart, oldPrefix.length - common.length, oldKeyPart.length);
      return newPart;
   }

   private static byte[] substring(byte[] key, int begin, int end) {
      if (end <= begin) return Util.EMPTY_BYTE_ARRAY;
      if (begin == 0 && end == key.length) {
         return key;
      }
      byte[] sub = new byte[end - begin];
      System.arraycopy(key, begin, sub, 0, end - begin);
      return sub;
   }

   private static byte[] commonPrefix(byte[] oldPrefix, byte[] newKey) {
      int i;
      for (i = 0; i < oldPrefix.length && i < newKey.length; ++i) {
         if (newKey[i] != oldPrefix[i]) break;
      }
      if (i == oldPrefix.length) {
         return oldPrefix;
      }
      if (i == newKey.length) {
         return newKey;
      }
      byte[] prefix = new byte[i];
      for (--i; i >= 0; --i) {
         prefix[i] = oldPrefix[i];
      }
      return prefix;
   }

   // Compares the two arrays. This is different from a regular compare that if the second array has more bytes than
   // the first but contains all the same bytes it is treated equal
   private static int compare(byte[] first, byte[] second, int secondLength) {
      for (int i = 0; i < secondLength; ++i) {
         if (i >= first.length) {
            return 1;
         }
         if (second[i] == first[i]) continue;
         return second[i] > first[i] ? 1 : -1;
      }
      return 0;
   }

   private static int compare(byte[] first, byte[] second) {
      for (int i = 0; i < first.length && i < second.length; ++i) {
         if (second[i] == first[i]) continue;
         return second[i] > first[i] ? i + 1 : -i - 1;
      }
      return second.length > first.length ? first.length + 1 : (second.length < first.length ? -second.length - 1 : 0);
   }

   private short headerLength() {
      int headerLength = INNER_NODE_HEADER_SIZE + prefix.length;
      assert headerLength <= Short.MAX_VALUE;
      return (short) headerLength;
   }

   private int contentLength() {
      if (contentLength >= 0) {
         return contentLength;
      }
      int sum = 0;
      for (byte[] keyPart : keyParts) {
         sum += 2 + keyPart.length;
      }
      if (innerNodes != null) {
         sum += INNER_NODE_REFERENCE_SIZE * innerNodes.length;
      } else if (leafNodes != null) {
         sum += LEAF_NODE_REFERENCE_SIZE * leafNodes.length;
      } else {
         throw new IllegalStateException();
      }
      assert sum <= Short.MAX_VALUE;
      return contentLength = (short) sum;
   }

   public short length() {
      if (totalLength >= 0) return totalLength;
      int totalLength = headerLength() + contentLength();
      assert totalLength >= 0 && totalLength <= Short.MAX_VALUE;
      return this.totalLength = (short) totalLength;
   }

   public static IndexNode emptyWithLeaves(Index.Segment segment) {
      return new IndexNode(segment, Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY_ARRAY, LeafNode.EMPTY_ARRAY);
   }

   private static IndexNode emptyWithInnerNodes(Index.Segment segment) {
      return new IndexNode(segment, Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY_ARRAY, new InnerNode[]{new InnerNode(-1L, (short) -1)});
   }

   static final OverwriteHook NOOP_HOOK = (int cacheSegment, boolean overwritten, int prevFile, int prevOffset) -> { };

   public interface OverwriteHook {
      default boolean check(int oldFile, int oldOffset) {
         return true;
      }

      void setOverwritten(int cacheSegment, boolean overwritten, int prevFile, int prevOffset);
   }

   static class InnerNode extends Index.IndexSpace {
      private volatile SoftReference<IndexNode> reference;

      InnerNode(long offset, short length) {
         super(offset, length);
      }

      InnerNode(IndexNode node) {
         super(node.offset, node.occupiedSpace);
         reference = new SoftReference<>(node);
      }

      IndexNode getIndexNode(Index.Segment segment) throws IOException {
         IndexNode node;
         if (reference == null || (node = reference.get()) == null) {
            synchronized (this) {
               if (reference == null || (node = reference.get()) == null) {
                  if (offset < 0) return null;
                  // Is this okay?
                  node = new IndexNode(segment, offset, length);
                  reference = new SoftReference<>(node);
                  if (log.isTraceEnabled()) {
                     log.trace("Loaded inner node from " + offset + " - " + length);
                  }
               }
            }
         }
         return node;
      }
   }

   private static class LeafNode extends EntryInfo {
      private static final LeafNode[] EMPTY_ARRAY = new LeafNode[0];
      private volatile SoftReference<EntryRecord> keyReference;

      LeafNode(int file, int offset, short numRecords, int cacheSegment) {
         super(file, offset, numRecords, cacheSegment);
      }

      public EntryRecord loadHeaderAndKey(FileProvider fileProvider) throws IOException, IndexNodeOutdatedException {
         return getHeaderAndKey(fileProvider, null);
      }

      private EntryRecord getHeaderAndKey(FileProvider fileProvider, FileProvider.Handle handle) throws IOException, IndexNodeOutdatedException {
         EntryRecord headerAndKey;
         if (keyReference == null || (headerAndKey = keyReference.get()) == null) {
            synchronized (this) {
               if (keyReference == null || (headerAndKey = keyReference.get()) == null) {
                  boolean ownHandle = false;
                  if (handle == null) {
                     ownHandle = true;
                     handle = fileProvider.getFile(file);
                     if (handle == null) {
                        throw new IndexNodeOutdatedException(file + ":" + offset + " (" + numRecords + ")");
                     }
                  }
                  try {
                     int readOffset = offset < 0 ? ~offset : offset;
                     EntryHeader header = EntryRecord.readEntryHeader(handle, readOffset);
                     if (header == null) {
                        throw new IllegalStateException("Error reading header from " + file + ":" + readOffset + " | " + handle.getFileSize());
                     }
                     byte[] key = EntryRecord.readKey(handle, header, readOffset);
                     if (key == null) {
                        throw new IllegalStateException("Error reading key from " + file + ":" + readOffset);
                     }
                     headerAndKey = new EntryRecord(header, key);
                     keyReference = new SoftReference<>(headerAndKey);
                  } finally {
                     if (ownHandle) {
                        handle.close();
                     }
                  }
               }
            }
         }
         assert headerAndKey.getKey() != null;
         return headerAndKey;
      }

      public EntryRecord loadRecord(FileProvider fileProvider, org.infinispan.commons.io.ByteBuffer key, TimeService timeService) throws IOException, IndexNodeOutdatedException {
         FileProvider.Handle handle = fileProvider.getFile(file);
         int readOffset = offset < 0 ? ~offset : offset;
         if (handle == null) {
            throw new IndexNodeOutdatedException(file + ":" + readOffset);
         }
         try {
            boolean trace = log.isTraceEnabled();
            EntryRecord headerAndKey = getHeaderAndKey(fileProvider, handle);
            if (key != null && !entryKeyEqualsBuffer(headerAndKey, key)) {
               if (trace) {
                  log.trace("Key on " + file + ":" + readOffset + " not matched.");
               }
               return null;
            }
            if (headerAndKey.getHeader().valueLength() <= 0) {
               if (trace) {
                  log.trace("Entry " + file + ":" + readOffset + " matched, it is a tombstone.");
               }
               return null;
            }
            if (timeService != null && headerAndKey.getHeader().expiryTime() > 0 && headerAndKey.getHeader().expiryTime() <= timeService.wallClockTime()) {
               if (trace) {
                  log.trace("Key on " + file + ":" + readOffset + " matched but expired.");
               }
               return null;
            }
            if (trace) {
               log.trace("Loaded from " + file + ":" + readOffset);
            }
            return headerAndKey.loadMetadataAndValue(handle, readOffset, key != null);
         } finally {
            handle.close();
         }
      }
   }

   private static class IndexNodeOutdatedException extends Exception {
      IndexNodeOutdatedException(String message) {
         super(message);
      }
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i <= keyParts.length; ++i) {
         sb.append('\n');
         if (leafNodes != null && i < leafNodes.length) {
            sb.append(" [").append(leafNodes[i].file).append(':').append(leafNodes[i].offset).append(':').append(leafNodes[i].cacheSegment).append("] ");
         } else {
            sb.append(" [").append(innerNodes[i].offset).append(':').append(innerNodes[i].length).append("] ");
         }
         if (i < keyParts.length) {
            sb.append(new String(concat(prefix, keyParts[i])));
         }
      }
      sb.append('\n');
      return sb.toString();
   }

   Flowable<EntryRecord> publish(IntSet cacheSegments, boolean loadValues) {
      long currentTime = segment.getTimeService().wallClockTime();

      int cacheSegmentSize = cacheSegments.size();
      if (cacheSegmentSize == 0) {
         return Flowable.empty();
      }

      // Needs defer as we mutate the deque so publisher can be subscribed to multiple times
      return Flowable.defer(() -> {
         // First sort all the cacheSegments by their unsigned numeric byte[] values.
         // This allows us to start at the left most node, and we can iterate within the nodes if the cacheSegments are
         // contiguous in the data
         Deque<byte[]> sortedSegmentPrefixes = cacheSegments.intStream()
               .filter(cacheSegment -> segment.index.sizePerSegment.get(cacheSegment) != 0)
               .mapToObj(cacheSegment -> {
                  byte[] segmentPrefix = new byte[UnsignedNumeric.sizeUnsignedInt(cacheSegment)];
                  UnsignedNumeric.writeUnsignedInt(segmentPrefix, 0, cacheSegment);
                  return segmentPrefix;
               }).sorted((o1, o2) -> IndexNode.compare(o2, o1))
               .collect(Collectors.toCollection(ArrayDeque::new));
         if (sortedSegmentPrefixes.isEmpty()) {
            return Flowable.empty();
         }

         return new FlowableCreate<>(emitter -> {
            // Set to true in 3 different cases: cacheSegment didn't map to next entry, emitter has no more requests or cancelled
            ByRef.Boolean done = new ByRef.Boolean(false);
            do {
               // Reset so we can loop
               done.set(false);
               recursiveNode(this, segment, sortedSegmentPrefixes, emitter, loadValues, currentTime, new ByRef.Boolean(false), done, false);
               // This handles two of the done cases - in which case we can't continue
               if (emitter.requested() == 0 || emitter.isCancelled()) {
                  return;
               }
            } while (done.get() && !sortedSegmentPrefixes.isEmpty());
            emitter.onComplete();
         }, BackpressureStrategy.ERROR);
      });
   }

   void recursiveNode(IndexNode node, Index.Segment segment, Deque<byte[]> segmentPrefixes, FlowableEmitter<EntryRecord> emitter,
         boolean loadValues, long currentTime, ByRef.Boolean foundData, ByRef.Boolean done, boolean firstNodeAttempted) throws IOException {
      Lock readLock = node.lock.readLock();
      readLock.lock();
      try {
         byte[] previousKey = null;
         int previousSegment = -1;
         if (node.innerNodes != null) {
            final int point = foundData.get() ? 0 : node.getIterationPoint(segmentPrefixes.getFirst(), -1);
            // Need to search all inner nodes starting from that point until we hit the last entry for the segment
            for (int i = point; !segmentPrefixes.isEmpty() && i < node.innerNodes.length && !done.get(); ++i) {
               recursiveNode(node.innerNodes[i].getIndexNode(segment), segment, segmentPrefixes, emitter, loadValues,
                     currentTime, foundData, done, i == point);
            }
         } else if (node.leafNodes != null) {
            int suggestedIteration;
            byte[] segmentPrefix = segmentPrefixes.getFirst();
            int cacheSegment = UnsignedNumeric.readUnsignedInt(segmentPrefix, 0);
            boolean firstData = !foundData.get();
            if (firstData) {
               suggestedIteration = node.getIterationPoint(segmentPrefix, cacheSegment);
               foundData.set(true);
            } else {
               suggestedIteration = 0;
            }

            for (int i = suggestedIteration; i < node.leafNodes.length; ++i) {
               LeafNode leafNode = node.leafNodes[i];
               if (leafNode.cacheSegment != cacheSegment) {
                  // The suggestion may be off by 1 if the page index prefix is longer than the segment but equal
                  if (i == suggestedIteration && firstData
                        && segmentPrefix.length == UnsignedNumeric.sizeUnsignedInt(cacheSegment)) {

                     // No entry for the given segment, make sure to try next segment
                     if (i != node.leafNodes.length - 1
                           && (i == node.keyParts.length ||
                           (i < node.keyParts.length &&
                                 compare(node.keyParts[i], segmentPrefix, Math.min(segmentPrefix.length, node.keyParts[i].length)) == 0)))
                        continue;

                     // The cache segment does not map to the current innerNode, we are at the end of the leafNodes,
                     // and this is the first innerNode attempted. We need to also check the first leaf of the next innerNode if present.
                     if (i == node.leafNodes.length - 1 && firstNodeAttempted) {
                        return;
                     }
                  }
                  segmentPrefixes.removeFirst();

                  // If the data maps to the next segment in our ordered queue, we can continue reading,
                  // otherwise we end and the retry will kick in
                  segmentPrefix = segmentPrefixes.peekFirst();
                  if (segmentPrefix != null) {
                     cacheSegment = UnsignedNumeric.readUnsignedInt(segmentPrefix, 0);
                  }
                  // Next cacheSegment doesn't match either, thus we have to retry with the next prefix
                  // Note that if segmentPrefix is null above, this will always be true
                  if (leafNode.cacheSegment != cacheSegment) {
                     done.set(true);
                     return;
                  }
               }

               EntryRecord record;
               try {
                  if (loadValues) {
                     log.tracef("Loading record for leafNode: %s", leafNode);
                     record = leafNode.loadRecord(segment.getFileProvider(), null, segment.getTimeService());
                  } else {
                     log.tracef("Loading header and key for leafNode: %s", leafNode);
                     record = leafNode.getHeaderAndKey(segment.getFileProvider(), null);
                  }
               } catch (IndexNodeOutdatedException e) {
                  // Current key was outdated, we have to try from the previous entry we saw (note it is skipped)
                  if (previousKey != null) {
                     byte[] currentIndexKey = Index.toIndexKey(previousSegment, previousKey);
                     segmentPrefixes.removeFirst();
                     segmentPrefixes.addFirst(currentIndexKey);
                  }
                  done.set(true);
                  return;
               }

               if (record != null && record.getHeader().valueLength() > 0) {
                  // It is possible that the very first looked up entry was a previously seen value and if so
                  // we must skip it if it is equal to not return it twice.
                  // The current segmentPrefix will match the element's key bytes excluding the segment bytes
                  if (firstData && i == suggestedIteration) {
                     int keyLength = record.getHeader().keyLength();
                     int lengthDiff = segmentPrefix.length - keyLength;
                     if (lengthDiff > 0) {
                        byte[] keyArray = record.getKey();
                        if (Util.arraysEqual(keyArray, 0, keyArray.length, segmentPrefix, lengthDiff, segmentPrefix.length)) {
                           continue;
                        }
                     }
                  }
                  long expiryTime = record.getHeader().expiryTime();
                  if (expiryTime < 0 || expiryTime > currentTime) {
                     emitter.onNext(record);

                     if (emitter.requested() == 0) {
                        // Store the current key as the next prefix when we can't retrieve more values, so
                        // the next request will get the next value after this one
                        byte[] currentIndexKey = Index.toIndexKey(cacheSegment, record.getKey());
                        segmentPrefixes.removeFirst();
                        segmentPrefixes.addFirst(currentIndexKey);
                        done.set(true);
                        return;
                     } else if (emitter.isCancelled()) {
                        done.set(true);
                        return;
                     }
                  }

                  previousKey = record.getKey();
                  previousSegment = cacheSegment;
               }
            }

            // We are continuing with the next innerNode, save the previous key, just in case we get an outdated
            // exception on the first entry
            if (previousKey != null) {
               byte[] currentIndexKey = Index.toIndexKey(previousSegment, previousKey);
               segmentPrefixes.removeFirst();
               segmentPrefixes.addFirst(currentIndexKey);
            }
         }
      } finally {
         readLock.unlock();
      }
   }
}
