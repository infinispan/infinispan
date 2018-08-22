package org.infinispan.persistence.sifs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.LogFactory;

/**
 * Keeps the entry positions persisted in a file. It consists of couple of segments, each for one modulo-range
 * of key's hashcodes (according to DataContainer's key equivalence configuration) - writes to each index segment
 * are performed by single thread, having multiple segments spreads the load between them.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Index {
   private static final Log log = LogFactory.getLog(Index.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int GRACEFULLY = 0x512ACEF0;
   private static final int DIRTY = 0xD112770C;
   private static final int INDEX_FILE_HEADER_SIZE = 30;

   private final String indexDir;
   private final FileProvider fileProvider;
   private final Compactor compactor;
   private final int minNodeSize;
   private final int maxNodeSize;
   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private final Segment[] segments;
   private final TimeService timeService;

   public Index(FileProvider fileProvider, String indexDir, int segments, int minNodeSize, int maxNodeSize,
                IndexQueue indexQueue, TemporaryTable temporaryTable, Compactor compactor,
                TimeService timeService) throws IOException {
      this.fileProvider = fileProvider;
      this.compactor = compactor;
      this.timeService = timeService;
      this.indexDir = indexDir;
      this.minNodeSize = minNodeSize;
      this.maxNodeSize = maxNodeSize;
      new File(indexDir).mkdirs();

      this.segments = new Segment[segments];
      for (int i = 0; i < segments; ++i) {
         this.segments[i] = new Segment(i, indexQueue.subQueue(i), temporaryTable);
      }
   }

   /**
    * @return True if the index was loaded from well persisted state
    */
   public boolean isLoaded() {
      for (Segment segment : segments) {
         if (!segment.loaded) return false;
      }
      return true;
   }

   public void start() {
      for (Segment segment : segments) {
         segment.start();
      }
   }

   /**
    * Get record or null if expired
    */
   public EntryRecord getRecord(Object key, byte[] serializedKey) throws IOException {
      int segment = (key.hashCode() & Integer.MAX_VALUE) % segments.length;
      lock.readLock().lock();
      try {
         return IndexNode.applyOnLeaf(segments[segment], serializedKey, segments[segment].rootReadLock(), IndexNode.ReadOperation.GET_RECORD);
      } finally {
         lock.readLock().unlock();
      }
   }

   /**
    * Get position or null if expired
    */
   public EntryPosition getPosition(Object key, byte[] serializedKey) throws IOException {
      int segment = (key.hashCode() & Integer.MAX_VALUE) % segments.length;
      lock.readLock().lock();
      try {
         return IndexNode.applyOnLeaf(segments[segment], serializedKey, segments[segment].rootReadLock(), IndexNode.ReadOperation.GET_POSITION);
      } finally {
         lock.readLock().unlock();
      }
   }

   /**
    * Get position + numRecords, without expiration
    */
   public EntryInfo getInfo(Object key, byte[] serializedKey) throws IOException {
      int segment = (key.hashCode() & Integer.MAX_VALUE) % segments.length;
      lock.readLock().lock();
      try {
         return IndexNode.applyOnLeaf(segments[segment], serializedKey, segments[segment].rootReadLock(), IndexNode.ReadOperation.GET_INFO);
      } finally {
         lock.readLock().unlock();
      }
   }

   public void clear() throws IOException {
      lock.writeLock().lock();
      try {
         ArrayList<CountDownLatch> pauses = new ArrayList<>();
         for (Segment seg : segments) {
            pauses.add(seg.pauseAndClear());
         }
         for (CountDownLatch pause : pauses) {
            pause.countDown();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
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

   public long size() throws InterruptedException {
      long size = 0;
      for (Segment seg : segments) {
         size += seg.size();
      }
      return size;
   }

   class Segment extends Thread {
      private final BlockingQueue<IndexRequest> indexQueue;
      private final TemporaryTable temporaryTable;
      private final TreeMap<Short, List<IndexSpace>> freeBlocks = new TreeMap<>();
      private final ReadWriteLock rootLock = new ReentrantReadWriteLock();
      private final File indexFileFile;
      private final boolean loaded;
      private FileChannel indexFile;
      private long indexFileSize;
      private AtomicLong size = new AtomicLong();

      private volatile IndexNode root;


      private Segment(int id, BlockingQueue<IndexRequest> indexQueue, TemporaryTable temporaryTable) throws IOException {
         super("BCS-IndexUpdater-" + id);
         this.setDaemon(true);
         this.indexQueue = indexQueue;
         this.temporaryTable = temporaryTable;

         this.indexFileFile = new File(indexDir, "index." + id);
         this.indexFile = new RandomAccessFile(indexFileFile, "rw").getChannel();
         indexFile.position(0);
         ByteBuffer buffer = ByteBuffer.allocate(INDEX_FILE_HEADER_SIZE);
         if (indexFile.size() >= INDEX_FILE_HEADER_SIZE && read(indexFile, buffer) && buffer.getInt(0) == GRACEFULLY) {
            long rootOffset = buffer.getLong(4);
            short rootOccupied = buffer.getShort(12);
            long freeBlocksOffset = buffer.getLong(14);
            size.set(buffer.getLong(22));
            root = new IndexNode(this, rootOffset, rootOccupied);
            loadFreeBlocks(freeBlocksOffset);
            indexFileSize = freeBlocksOffset;
            loaded = true;
         } else {
            this.indexFile.truncate(0);
            root = IndexNode.emptyWithLeaves(this);
            loaded = false;
            // reserve space for shutdown
            indexFileSize = INDEX_FILE_HEADER_SIZE;
         }
         buffer.putInt(0, DIRTY);
         buffer.position(0);
         buffer.limit(4);
         indexFile.position(0);
         write(indexFile, buffer);
      }

      private void write(FileChannel indexFile, ByteBuffer buffer) throws IOException {
         do {
            int written = indexFile.write(buffer);
            if (written < 0) {
               throw new IllegalStateException("Cannot write to index file!");
            }
         } while (buffer.position() < buffer.limit());
      }

      private boolean read(FileChannel indexFile, ByteBuffer buffer) throws IOException {
         do {
            int read = indexFile.read(buffer);
            if (read < 0) {
               return false;
            }
         } while (buffer.position() < buffer.limit());
         return true;
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
               if (trace) log.trace("Indexing " + request);
               IndexNode.OverwriteHook overwriteHook;
               IndexNode.RecordChange recordChange;
               switch (request.getType()) {
                  case CLEAR:
                     IndexRequest cleared;
                     while ((cleared = indexQueue.poll()) != null) {
                        cleared.setResult(false);
                     }
                     CountDownLatch pause = new CountDownLatch(1);
                     request.setResult(pause);
                     log.debug("Waiting for cleared");
                     pause.await();
                     continue;
                  case DELETE_FILE:
                     // the last segment that processes the delete request actually deletes the file
                     if (request.countDown()) {
                        fileProvider.deleteFile(request.getFile());
                        compactor.releaseStats(request.getFile());
                     }
                     continue;
                  case STOP:
                     assert indexQueue.poll() == null;
                     shutdown();
                     return;
                  case GET_SIZE :
                     request.setResult(size.get());
                     continue;
                  case MOVED:
                     recordChange = IndexNode.RecordChange.MOVE;
                     overwriteHook = new IndexNode.OverwriteHook() {
                        @Override
                        public boolean check(int oldFile, int oldOffset) {
                           return oldFile == request.getPrevFile() && oldOffset == request.getPrevOffset();
                        }

                        @Override
                        public void setOverwritten(boolean overwritten, int prevFile, int prevOffset) {
                           if (overwritten && request.getOffset() < 0 && request.getPrevOffset() >= 0) {
                              size.decrementAndGet();
                           }
                        }
                     };
                     break;
                  case UPDATE:
                     recordChange = IndexNode.RecordChange.INCREASE;
                     overwriteHook = new IndexNode.OverwriteHook() {
                        @Override
                        public void setOverwritten(boolean overwritten, int prevFile, int prevOffset) {
                           request.setResult(overwritten);
                           if (request.getOffset() >= 0 && prevOffset < 0) {
                              size.incrementAndGet();
                           } else if (request.getOffset() < 0 && prevOffset >= 0) {
                              size.decrementAndGet();
                           }
                        }
                     };
                     break;
                  case DROPPED:
                     recordChange = IndexNode.RecordChange.DECREASE;
                     overwriteHook = new IndexNode.OverwriteHook() {
                        @Override
                        public void setOverwritten(boolean overwritten, int prevFile, int prevOffset) {
                           if (request.getPrevFile() == prevFile && request.getPrevOffset() == prevOffset) {
                              size.decrementAndGet();
                           }
                        }
                     };
                     break;
                  case FOUND_OLD:
                     recordChange = IndexNode.RecordChange.INCREASE_FOR_OLD;
                     overwriteHook = IndexNode.OverwriteHook.NOOP;
                     break;
                  default:
                     throw new IllegalArgumentException(request.toString());
               }
               try {
                  IndexNode.setPosition(root, request.getSerializedKey(), request.getFile(), request.getOffset(),
                        request.getSize(), overwriteHook, recordChange);
               } catch (IllegalStateException e) {
                  throw new IllegalStateException(request.toString(), e);
               }
               temporaryTable.removeConditionally(request.getKey(), request.getFile(), request.getOffset());
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
         } catch (Throwable e) {
            log.errorInIndexUpdater(e);
         } finally {
            try {
               indexFile.close();
            } catch (IOException e) {
               log.failedToCloseIndex(e);
            }
         }
      }

      private void shutdown() throws IOException {
         IndexSpace rootSpace = allocateIndexSpace(root.length());
         root.store(rootSpace);
         indexFile.position(indexFileSize);
         ByteBuffer buffer = ByteBuffer.allocate(4);
         buffer.putInt(0, freeBlocks.size());
         write(indexFile, buffer);
         for (Map.Entry<Short, List<IndexSpace>> entry : freeBlocks.entrySet()) {
            List<IndexSpace> list = entry.getValue();
            int requiredSize = 8 + list.size() * 10;
            buffer = buffer.capacity() < requiredSize ? ByteBuffer.allocate(requiredSize) : buffer;
            buffer.position(0);
            buffer.limit(requiredSize);
            // TODO: change this to short
            buffer.putInt(entry.getKey());
            buffer.putInt(list.size());
            for (IndexSpace space : list) {
               buffer.putLong(space.offset);
               buffer.putShort(space.length);
            }
            buffer.flip();
            write(indexFile, buffer);
         }
         int headerWithoutMagic = INDEX_FILE_HEADER_SIZE - 4;
         buffer = buffer.capacity() < headerWithoutMagic ? ByteBuffer.allocate(headerWithoutMagic) : buffer;
         buffer.position(0);
         // we need to set limit ahead, otherwise the putLong could throw IndexOutOfBoundsException
         buffer.limit(headerWithoutMagic);
         buffer.putLong(0, rootSpace.offset);
         buffer.putShort(8, rootSpace.length);
         buffer.putLong(10, indexFileSize);
         buffer.putLong(18, size.get());
         indexFile.position(4);
         write(indexFile, buffer);
         buffer.position(0);
         buffer.limit(4);
         buffer.putInt(0, GRACEFULLY);
         indexFile.position(0);
         write(indexFile, buffer);
      }

      private void loadFreeBlocks(long freeBlocksOffset) throws IOException {
         indexFile.position(freeBlocksOffset);
         ByteBuffer buffer = ByteBuffer.allocate(8);
         buffer.limit(4);
         if (!read(indexFile, buffer)) {
            throw new IOException("Cannot read free blocks lists!");
         }
         int numLists = buffer.getInt(0);
         for (int i = 0; i < numLists; ++i) {
            buffer.position(0);
            buffer.limit(8);
            if (!read(indexFile, buffer)) {
               throw new IOException("Cannot read free blocks lists!");
            }
            // TODO: change this to short
            int blockLength = buffer.getInt(0);
            assert blockLength <= Short.MAX_VALUE;
            int listSize = buffer.getInt(4);
            int requiredSize = 10 * listSize;
            buffer = buffer.capacity() < requiredSize ? ByteBuffer.allocate(requiredSize) : buffer;
            buffer.position(0);
            buffer.limit(requiredSize);
            if (!read(indexFile, buffer)) {
               throw new IOException("Cannot read free blocks lists!");
            }
            buffer.flip();
            ArrayList<IndexSpace> list = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; ++j) {
               list.add(new IndexSpace(buffer.getLong(), buffer.getShort()));
            }
            freeBlocks.put((short) blockLength, list);
         }
      }

      CountDownLatch pauseAndClear() throws InterruptedException, IOException {
         IndexRequest clear = IndexRequest.clearRequest();
         indexQueue.put(clear);
         CountDownLatch pause = (CountDownLatch) clear.getResult();
         root = IndexNode.emptyWithLeaves(this);
         indexFile.truncate(0);
         indexFileSize = INDEX_FILE_HEADER_SIZE;
         freeBlocks.clear();
         size.set(0);
         return pause;
      }

      public long size() throws InterruptedException {
         IndexRequest sizeRequest = IndexRequest.sizeRequest();
         indexQueue.put(sizeRequest);
         return (Long) sizeRequest.getResult();
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
      IndexSpace allocateIndexSpace(short length) {
         Map.Entry<Short, List<IndexSpace>> entry = freeBlocks.ceilingEntry(length);
         if (entry == null || entry.getValue().isEmpty()) {
            long oldSize = indexFileSize;
            indexFileSize += length;
            return new IndexSpace(oldSize, length);
         } else {
            return entry.getValue().remove(entry.getValue().size() - 1);
         }
      }

      // this should be accessed only from the updater thread
      void freeIndexSpace(long offset, short length) {
         if (length <= 0) throw new IllegalArgumentException("Offset=" + offset + ", length=" + length);
         // TODO: fragmentation!
         // TODO: memory bounds!
         if (offset + length < indexFileSize) {
            freeBlocks.computeIfAbsent(length, k -> new ArrayList<>()).add(new IndexSpace(offset, length));
         } else {
            indexFileSize -= length;
            try {
               indexFile.truncate(indexFileSize);
            } catch (IOException e) {
               log.cannotTruncateIndex(e);
            }
         }
      }

      Lock rootReadLock() {
         return rootLock.readLock();
      }

      void stopOperations() throws InterruptedException {
         indexQueue.put(IndexRequest.stopRequest());
         this.join();
      }

      public TimeService getTimeService() {
         return timeService;
      }
   }

   /**
    * Offset-length pair
    */
   static class IndexSpace {
      protected long offset;
      protected short length;

      IndexSpace(long offset, short length) {
         this.offset = offset;
         this.length = length;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || !(o instanceof IndexSpace)) return false;

         IndexSpace innerNode = (IndexSpace) o;

         return length == innerNode.length && offset == innerNode.offset;
      }

      @Override
      public int hashCode() {
         int result = (int) (offset ^ (offset >>> 32));
         result = 31 * result + length;
         return result;
      }

      @Override
      public String toString() {
         return String.format("[%d-%d(%d)]", offset, offset + length, length);
      }
   }

}
