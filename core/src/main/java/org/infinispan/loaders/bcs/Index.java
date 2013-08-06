package org.infinispan.loaders.bcs;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Keeps the entry positions persisted in a file. It consists of couple of segments, each for one modulo-range
 * of key's hashcodes - writes to each index segment are performed by single thread, having multiple segments
 * spreads the load between them.
  *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Index {
   private static final Log log = LogFactory.getLog(Index.class);

   private final String indexDir;
   private final FileProvider fileProvider;
   private final Compactor compactor;
   private final int minNodeSize;
   private final int maxNodeSize;
   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final Segment[] segments;


   public Index(FileProvider fileProvider, String indexDir, int segments, int minNodeSize, int maxNodeSize,
                IndexQueue indexQueue,
                TemporaryTable temporaryTable,
                Compactor compactor) throws IOException {
      this.fileProvider = fileProvider;
      this.compactor = compactor;
      this.indexDir = indexDir;
      this.minNodeSize = minNodeSize;
      this.maxNodeSize = maxNodeSize;
      new File(indexDir).mkdirs();

      this.segments = new Segment[segments];
      for (int i = 0; i < segments; ++i) {
         this.segments[i] = new Segment(i, indexQueue.subQueue(i), temporaryTable);
      }
   }

   public byte[] getValue(Object key, byte[] serializedKey) throws IOException {
      int segment = Math.abs(key.hashCode()) % segments.length;
      lock.readLock().lock();
      try {
         return IndexNode.applyOnLeaf(segments[segment], serializedKey, segments[segment].rootReadLock(), IndexNode.ReadOperation.READ_VALUE);
      } finally {
         lock.readLock().unlock();
      }
   }

   public EntryPosition getPosition(Object key, byte[] serializedKey) throws IOException {
      int segment = Math.abs(key.hashCode()) % segments.length;
      lock.readLock().lock();
      try {
         return IndexNode.applyOnLeaf(segments[segment], serializedKey, segments[segment].rootReadLock(), IndexNode.ReadOperation.GET_POSITION);
      } finally {
         lock.readLock().unlock();
      }
   }


   public void clear() throws IOException {
      lock.writeLock().lock();
      try {
         ArrayList<CountDownLatch> pauses = new ArrayList<CountDownLatch>();
         for (Segment seg : segments) {
            pauses.add(seg.pauseAndClear());
         }
         for (CountDownLatch pause : pauses) {
            pause.countDown();
         }
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      } finally {
         lock.writeLock().unlock();
      }
   }

   public void stopOperations() throws InterruptedException {
      for (Segment seg : segments) {
         seg.stopOperations();
      }
   }

   class Segment extends Thread {
      private final BlockingQueue<IndexRequest> indexQueue;
      private final TemporaryTable temporaryTable;
      private final TreeMap<Integer, List<IndexSpace>> freeBlocks = new TreeMap<Integer, List<IndexSpace>>();
      private final ReadWriteLock rootLock = new ReentrantReadWriteLock();
      private FileChannel indexFile;
      private long indexFileSize = 0;
      private volatile IndexNode root = IndexNode.emptyWithLeaves(this);

      private Segment(int id, BlockingQueue<IndexRequest> indexQueue, TemporaryTable temporaryTable) throws IOException {
         super("BCS-IndexUpdater-" + id);
         this.setDaemon(true);
         this.indexQueue = indexQueue;
         this.temporaryTable = temporaryTable;

         this.indexFile = new RandomAccessFile(new File(indexDir, "index." + id), "rw").getChannel();
         this.indexFile.truncate(0);

         start();
      }

      @Override
      public void run() {
         try {
            int counter = 0;
            while (true) {
               if (++counter % 30000 == 0) {
                  log.debug("Queue size is " + indexQueue.size());
               }
               final IndexRequest request = indexQueue.take();
               if (request.isClear()) {
                  IndexRequest cleared;
                  while ((cleared = indexQueue.poll()) != null) {
                     cleared.setResult(false);
                  }
                  CountDownLatch pause = new CountDownLatch(1);
                  request.setResult(pause);
                  log.debug("Waiting for cleared " + System.nanoTime());
                  pause.await();
                  continue;
               } else if (request.isDelete()) {
                  // the last segment that processes the delete request actually deletes the file
                  if (request.countDown()) {
                     fileProvider.deleteFile(request.getFile());
                     compactor.releaseStats(request.getFile());
                  }
                  continue;
               } else if (request.isStop()) {
                  break;
               }

               IndexNode.OverwriteHook overwriteHook;
               if (request.isCompareAndSet()) {
                  overwriteHook = new IndexNode.OverwriteHook() {
                     @Override
                     public boolean check(int oldFile, int oldOffset) {
                        return oldFile == request.getPrevFile() && oldOffset == request.getPrevOffset();
                     }
                  };
               } else {
                  overwriteHook = new IndexNode.OverwriteHook() {
                     @Override
                     public void setOverwritten(boolean overwritten) {
                        request.setResult(overwritten);
                     }
                  };
               }
               try {
                  IndexNode.setPosition(root, request.getSerializedKey(), request.getFile(), request.getOffset(), request.getSize(), overwriteHook);
               } catch (IllegalStateException e) {
                  throw new IllegalStateException(request.toString(), e);
               }
               request.setResult(false);
               temporaryTable.removeConditionally(request.getKey(), request.getFile(), request.getOffset());
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         } finally {
            // TODO: notify all that this component has ceased
         }
      }

      public CountDownLatch pauseAndClear() throws InterruptedException, IOException {
         IndexRequest clear = IndexRequest.clearRequest();
         indexQueue.put(clear);
         CountDownLatch pause = (CountDownLatch) clear.getResult();
         root = IndexNode.emptyWithLeaves(this);
         indexFile.truncate(0);
         indexFileSize = 0;
         freeBlocks.clear();
         return pause;
      }

      public FileChannel getIndexFile() {
         return indexFile;
      }

      public FileProvider getFileProvider() {
         return fileProvider;
      }

      public Compactor getCompactor() {
         return compactor;
      }

      public IndexNode getRoot() {
         // this has to be called with rootLock locked!
         return root;
      }

      public void setRoot(IndexNode root) {
         rootLock.writeLock().lock();
         this.root = root;
         rootLock.writeLock().unlock();
      }

      public int getMaxNodeSize() {
         return maxNodeSize;
      }

      public int getMinNodeSize() {
         return minNodeSize;
      }

      // this should be accessed only from the updater thread
      IndexSpace allocateIndexSpace(int length) {
         Map.Entry<Integer, List<IndexSpace>> entry = freeBlocks.ceilingEntry(length);
         if (entry == null || entry.getValue().isEmpty()) {
            long oldSize = indexFileSize;
            indexFileSize += length;
            return new IndexSpace(oldSize, length);
         } else {
            return entry.getValue().remove(entry.getValue().size() - 1);
         }
      }

      // this should be accessed only from the updater thread
      void freeIndexSpace(long offset, int length) {
         if (length <= 0) throw new IllegalArgumentException("Offset=" + offset + ", length=" + length);
         // TODO: fragmentation!
         // TODO: memory bounds!
         if (offset + length < indexFileSize) {
            List<IndexSpace> list = freeBlocks.get(length);
            if (list == null) {
               freeBlocks.put(length, list = new ArrayList<IndexSpace>());
            }
            list.add(new IndexSpace(offset, length));
         } else {
            indexFileSize -= length;
            try {
               indexFile.truncate(indexFileSize);
            } catch (IOException e) {
               log.warn("Cannot truncate index", e);
            }
         }
      }

      public Lock rootReadLock() {
         return rootLock.readLock();
      }

      public void stopOperations() throws InterruptedException {
         indexQueue.put(IndexRequest.stopRequest());
         this.join();
      }
   }

   /**
    * Offset-length pair
    */
   static class IndexSpace {
      protected long offset;
      protected int length;

      public IndexSpace(long offset, int length) {
         this.offset = offset;
         this.length = length;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || o instanceof IndexSpace) return false;

         IndexSpace innerNode = (IndexSpace) o;

         if (length != innerNode.length) return false;
         if (offset != innerNode.offset) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = (int) (offset ^ (offset >>> 32));
         result = 31 * result + length;
         return result;
      }
   }

}
