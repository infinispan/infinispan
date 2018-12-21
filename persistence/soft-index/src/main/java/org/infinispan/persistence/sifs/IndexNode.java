package org.infinispan.persistence.sifs;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.LogFactory;

/**
 * The recursive index structure. References to children are held in soft references,
 * which allows JVM-handled caching and reduces the amount of reads required while
 * evading OOMs if the index gets too big.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class IndexNode {
   private static final Log log = LogFactory.getLog(IndexNode.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final byte HAS_LEAVES = 1;
   private static final byte HAS_NODES = 2;
   private static final int INNER_NODE_HEADER_SIZE = 5;
   private static final int INNER_NODE_REFERENCE_SIZE = 10;
   private static final int LEAF_NODE_REFERENCE_SIZE = 10;

   public static final int RESERVED_SPACE
         = INNER_NODE_HEADER_SIZE + 2 * Math.max(INNER_NODE_REFERENCE_SIZE, LEAF_NODE_REFERENCE_SIZE);

   private Index.Segment segment;
   private byte[] prefix;
   private byte[][] keyParts;
   private InnerNode[] innerNodes;
   private LeafNode[] leafNodes;
   private ReadWriteLock lock = new ReentrantReadWriteLock();
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
            leafNodes[i] = new LeafNode(buffer.getInt(), buffer.getInt(), buffer.getShort());
         }
      } else if ((flags & HAS_NODES) != 0){
         innerNodes = new InnerNode[numKeyParts + 1];
         for (int i = 0; i < numKeyParts + 1; ++i) {
            innerNodes[i] = new InnerNode(buffer.getLong(), buffer.getShort());
         }
      } else {
         // the default
         leafNodes = LeafNode.EMPTY_ARRAY;
      }

      if (trace) {
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
         }
      }
      buffer.flip();
      segment.getIndexFile().write(buffer, offset);

      if (trace) {
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

   public enum ReadOperation {
      GET_RECORD {
         @Override
         protected EntryRecord apply(LeafNode leafNode, byte[] key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            return leafNode.loadRecord(fileProvider, key, timeService);
         }
      },
      GET_POSITION {
         @Override
         protected EntryPosition apply(LeafNode leafNode, byte[] key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            EntryRecord hak = leafNode.loadHeaderAndKey(fileProvider);
            if (Arrays.equals(hak.getKey(), key)) {
               if (hak.getHeader().expiryTime() > 0 && hak.getHeader().expiryTime() <= timeService.wallClockTime()) {
                  if (trace) {
                     log.tracef("Found node on %d:%d but it is expired", leafNode.file, leafNode.offset);
                  }
                  return null;
               }
               return leafNode;
            } else {
               if (trace) {
                  log.tracef("Found node on %d:%d but key does not match", leafNode.file, leafNode.offset);
               }
            }
            return null;
         }
      },
      GET_INFO {
         @Override
         protected EntryInfo apply(LeafNode leafNode, byte[] key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException {
            EntryRecord hak = leafNode.loadHeaderAndKey(fileProvider);
            if (Arrays.equals(hak.getKey(), key)) {
               return leafNode;
            } else {
               if (trace) {
                  log.tracef("Found node on %d:%d but key does not match", leafNode.file, leafNode.offset);
               }
               return null;
            }
         }
      };

      protected abstract <T> T apply(LeafNode leafNode, byte[] key, FileProvider fileProvider, TimeService timeService) throws IOException, IndexNodeOutdatedException;
   }

   public static <T> T applyOnLeaf(Index.Segment segment, byte[] key, Lock rootLock, ReadOperation operation) throws IOException {
      int attempts = 0;
      for (;;) {
         rootLock.lock();
         IndexNode node = segment.getRoot();
         Lock parentLock = rootLock, currentLock = null;
         try {
            while (node.innerNodes != null) {
               currentLock = node.lock.readLock();
               currentLock.lock();
               parentLock.unlock();
               parentLock = currentLock;
               int insertionPoint = node.getInsertionPoint(key);
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
            int insertionPoint = node.getInsertionPoint(key);
            return operation.apply(node.leafNodes[insertionPoint], key, segment.getFileProvider(), segment.getTimeService());
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

   public static void setPosition(IndexNode root, byte[] key, int file, int offset, int size, OverwriteHook overwriteHook, RecordChange recordChange) throws IOException {
      IndexNode node = root;
      Stack<Path> stack = new Stack<>();
      while (node.innerNodes != null) {
         int insertionPoint = node.getInsertionPoint(key);
         stack.push(new Path(node, insertionPoint));
         if (trace) {
            log.tracef("Pushed %08x (length %d, %d children) to stack (insertion point %d)", System.identityHashCode(node), node.length(), node.innerNodes.length, insertionPoint);
         }
         node = node.innerNodes[insertionPoint].getIndexNode(root.segment);
      }
      IndexNode copy = node.copyWith(key, file, offset, size, overwriteHook, recordChange);
      if (copy == node) {
         // no change was executed
         return;
      }
      if (trace) {
         log.tracef("Created %08x (length %d) from %08x (length %d), stack size %d",
               System.identityHashCode(copy), copy.length(), System.identityHashCode(node), node.length(), stack.size());
      }

      Stack<IndexNode> garbage = new Stack<>();
      try {
         JoinSplitResult result = manageLength(root.segment, stack, node, copy, garbage);
         if (result == null) {
            return;
         }
         if (trace) {
            log.tracef("Created (1) %d new nodes, GC %08x", result.newNodes.size(), System.identityHashCode(node));
         }
         garbage.push(node);
         for (;;) {
            if (stack.isEmpty()) {
               IndexNode newRoot;
               if (result.newNodes.size() == 1) {
                  newRoot = result.newNodes.get(0);
                  if (trace) {
                     log.tracef("Setting new root %08x (index has shrunk)", System.identityHashCode(newRoot));
                  }
               } else {
                  newRoot = IndexNode.emptyWithInnerNodes(root.segment).copyWith(0, 0, result.newNodes);
                  if (trace) {
                     log.tracef("Setting new root %08x (index has grown)", System.identityHashCode(newRoot));
                  }
               }
               newRoot.segment.setRoot(newRoot);
               return;
            }
            Path path = stack.pop();
            copy = path.node.copyWith(result.from, result.to, result.newNodes);
            if (trace) {
               log.tracef("Created %08x (length %d) from %08x with the %d new nodes (%d - %d)",
                     System.identityHashCode(copy), copy.length(), System.identityHashCode(path.node), result.newNodes.size(), result.from, result.to);
            }
            result = manageLength(path.node.segment, stack, path.node, copy, garbage);
            if (result == null) {
               if (trace) {
                  log.tracef("No more index updates required");
               }
               return;
            }
            if (trace) {
               log.tracef("Created (2) %d new nodes, GC %08x", result.newNodes.size(), System.identityHashCode(path.node));
            }
            garbage.push(path.node);
         }
      } finally {
         while (!garbage.isEmpty()) {
            IndexNode oldNode = garbage.pop();
            // this will be never unlocked, if the algorithm is correct, this node should be GC'ed soon.
            oldNode.lock.writeLock().lock();
            if (oldNode.offset >= 0) {
               oldNode.segment.freeIndexSpace(oldNode.offset, oldNode.occupiedSpace);
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

   private static JoinSplitResult manageLength(Index.Segment segment, Stack<Path> stack, IndexNode node, IndexNode copy, Stack<IndexNode> garbage) throws IOException {
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
      newPrefix = commonPrefix(newPrefix == null ? left.prefix : newPrefix, middleKey);
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
            if (hak != null && hak.getKey() != null) return hak.getKey();
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
            if (hak != null && hak.getKey() != null) return hak.getKey();
         }
      }
      return null;
   }

   /**
    * Called on the most bottom node
    */
   private IndexNode copyWith(byte[] key, int file, int offset, int size, OverwriteHook overwriteHook, RecordChange recordChange) throws IOException {
      if (leafNodes == null) throw new IllegalArgumentException();
      byte[] newPrefix;
      byte[][] newKeyParts;
      LeafNode[] newLeafNodes;
      if (leafNodes.length == 0) {
         overwriteHook.setOverwritten(false, -1, -1);
         if (overwriteHook.check(-1, -1)) {
            return new IndexNode(segment, prefix, keyParts, new LeafNode[]{ new LeafNode(file, offset, (short) 1)});
         } else {
            segment.getCompactor().free(file, size);
            return this;
         }
      }

      int insertPart = getInsertionPoint(key);
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

      EntryRecord hak;
      try {
         hak = oldLeafNode.loadHeaderAndKey(segment.getFileProvider());
      } catch (IndexNodeOutdatedException e) {
         throw new IllegalStateException("Index cannot be outdated for segment updater thread", e);
      }
      int keyComp = compare(hak.getKey(), key);
      if (keyComp == 0) {
         if (numRecords > 0) {
            if (overwriteHook.check(oldLeafNode.file, oldLeafNode.offset)) {
               newPrefix = prefix;
               newKeyParts = keyParts;
               newLeafNodes = new LeafNode[leafNodes.length];
               System.arraycopy(leafNodes, 0, newLeafNodes, 0, leafNodes.length);
               // Do not update the file and offset for DROPPED IndexRequests
               if (recordChange == RecordChange.INCREASE || recordChange == RecordChange.MOVE) {
                  if (trace) {
                     log.trace(String.format("Overwriting %d:%d with %d:%d (%d)",
                           oldLeafNode.file, oldLeafNode.offset, file, offset, numRecords));
                  }
                  newLeafNodes[insertPart] = new LeafNode(file, offset, numRecords);
                  segment.getCompactor().free(oldLeafNode.file, hak.getHeader().totalLength());
               } else {
                  if (trace) {
                     log.trace(String.format("Updating num records for %d:%d to %d", oldLeafNode.file, oldLeafNode.offset, numRecords));
                  }
                  newLeafNodes[insertPart] = new LeafNode(oldLeafNode.file, oldLeafNode.offset, numRecords);
               }
               overwriteHook.setOverwritten(true, oldLeafNode.file, oldLeafNode.offset);
            } else {
               overwriteHook.setOverwritten(false, -1, -1);
               segment.getCompactor().free(file, size);
               return this;
            }
         } else {
            overwriteHook.setOverwritten(true, oldLeafNode.file, oldLeafNode.offset);
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
         overwriteHook.setOverwritten(false, -1, -1);
         // We have to insert the record even if this is a delete request and the key was not found
         // because otherwise we would have incorrect numRecord count. Eventually, Compactor will
         // drop the tombstone and update index, removing this node
         if (keyParts.length == 0) {
            // TODO: we may use unnecessarily long keys here and the key is never shortened
            newPrefix = keyComp > 0 ? key : hak.getKey();
         } else {
            newPrefix = commonPrefix(prefix, key);
         }
         newKeyParts = new byte[keyParts.length + 1][];
         newLeafNodes = new LeafNode[leafNodes.length + 1];
         copyKeyParts(keyParts, 0, newKeyParts, 0, insertPart, prefix, newPrefix);
         copyKeyParts(keyParts, insertPart, newKeyParts, insertPart + 1, keyParts.length - insertPart, prefix, newPrefix);
         if (keyComp > 0) {
            newKeyParts[insertPart] = substring(key, newPrefix.length, keyComp);
            System.arraycopy(leafNodes, 0, newLeafNodes, 0, insertPart + 1);
            System.arraycopy(leafNodes, insertPart + 1, newLeafNodes, insertPart + 2, leafNodes.length - insertPart - 1);
            newLeafNodes[insertPart + 1] = new LeafNode(file, offset, (short) 1);
         } else {
            newKeyParts[insertPart] = substring(hak.getKey(), newPrefix.length, -keyComp);
            System.arraycopy(leafNodes, 0, newLeafNodes, 0, insertPart);
            System.arraycopy(leafNodes, insertPart, newLeafNodes, insertPart + 1, leafNodes.length - insertPart);
            newLeafNodes[insertPart] = new LeafNode(file, offset, (short) 1);
         }
      }
      return new IndexNode(segment, newPrefix, newKeyParts, newLeafNodes);
   }

   private int getInsertionPoint(byte[] key) {
      int comp = compare(prefix, key, prefix.length);
      int insertionPoint;
      if (comp < 0) {
         insertionPoint = 0;
      } else if (comp > 0) {
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

   private static int compare(byte[] first, byte[] second, int length) {
      for (int i = 0; i < length; ++i) {
         if (i >= second.length) {
            return -1;
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
      return new IndexNode(segment, Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY_ARRAY, new InnerNode[]{ new InnerNode(-1L, (short) -1) });
   }

   public static class OverwriteHook {
      public static final OverwriteHook NOOP = new OverwriteHook();

      public boolean check(int oldFile, int oldOffset) {
         return true;
      }
      public void setOverwritten(boolean overwritten, int prevFile, int prevOffset) {
      }
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
                  node = new IndexNode(segment, offset, length);
                  reference = new SoftReference<>(node);
                  if (trace) {
                     log.trace("Loaded inner node from " + offset + " - " + length);
                  }
               }
            }
         }
         return node;
      }
   }

   private static class LeafNode extends EntryInfo {
      private static LeafNode[] EMPTY_ARRAY = new LeafNode[0];
      private volatile SoftReference<EntryRecord> keyReference;

      LeafNode(int file, int offset, short numRecords) {
         super(file, offset, numRecords);
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

      public EntryRecord loadRecord(FileProvider fileProvider, byte[] key, TimeService timeService) throws IOException, IndexNodeOutdatedException {
         FileProvider.Handle handle = fileProvider.getFile(file);
         int readOffset = offset < 0 ? ~offset : offset;
         if (handle == null) {
            throw new IndexNodeOutdatedException(file + ":" + readOffset);
         }
         try {
            EntryRecord headerAndKey = getHeaderAndKey(fileProvider, handle);
            if (!Arrays.equals(key, headerAndKey.getKey())) {
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
            if (headerAndKey.getHeader().expiryTime() > 0 && headerAndKey.getHeader().expiryTime() <= timeService.wallClockTime()) {
               if (trace) {
                  log.trace("Key on " + file + ":" + readOffset + " matched but expired.");
               }
               return null;
            }
            if (trace) {
               log.trace("Loaded from " + file + ":" + readOffset);
            }
            return headerAndKey.loadMetadataAndValue(handle, readOffset);
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
         if (leafNodes != null) {
            sb.append(" [").append(leafNodes[i].file).append(':').append(leafNodes[i].offset).append("] ");
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
}
