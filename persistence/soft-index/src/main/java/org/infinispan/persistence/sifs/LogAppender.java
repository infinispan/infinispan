package org.infinispan.persistence.sifs;

import java.util.concurrent.BlockingQueue;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This component has the only thread that polls the queue with requests to write some entry into the cache store.
 * It writes the records to append-only log files, inserts the entry position into TemporaryTable and queues the position
 * to be persisted in Index.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class LogAppender extends Thread {
   private static final Log log = LogFactory.getLog(LogAppender.class);

   private final SyncProcessingQueue<LogRequest> queue;
   private final BlockingQueue<IndexRequest> indexQueue;
   private final boolean syncWrites;
   private final TemporaryTable temporaryTable;
   private final int maxFileSize;
   private final Compactor compactor;
   private final FileProvider fileProvider;
   private LogRequest pausedRequest;
   private long seqId = 0;

   LogAppender(SyncProcessingQueue<LogRequest> inboundQueue,
               BlockingQueue<IndexRequest> indexQueue,
               TemporaryTable temporaryTable,
               Compactor compactor,
               FileProvider fileProvider, boolean syncWrites, int maxFileSize) {
      super("BCS-LogAppender");
      this.setDaemon(true);
      this.queue = inboundQueue;
      this.indexQueue = indexQueue;
      this.temporaryTable = temporaryTable;
      this.compactor = compactor;
      this.fileProvider = fileProvider;
      this.syncWrites = syncWrites;
      this.maxFileSize = maxFileSize;
      start();
   }

   public void setSeqId(long seqId) {
      this.seqId = seqId;
   }

   public void pause() throws InterruptedException {
      LogRequest pause = LogRequest.pauseRequest();
      queue.pushAndWait(pause);
      pausedRequest = pause;
   }

   public void clearAndPause() throws InterruptedException {
      LogRequest clear = LogRequest.clearRequest();
      queue.pushAndWait(clear);
      pausedRequest = clear;
   }

   public void resumeAfterPause() {
      pausedRequest.resume();
      pausedRequest = null;
   }

   @Override
   public void run() {
      try {
         FileProvider.Log logFile = fileProvider.getFileForLog();
         int currentOffset = 0;
         while (true) {
            LogRequest request = queue.pop();
            if (request != null) {
               if (request.isClear()) {
                  logFile.close();
                  queue.notifyNoWait();
                  request.pause();
                  currentOffset = 0;
                  logFile = fileProvider.getFileForLog();
                  log.debug("Appending records to " + logFile.fileId);
                  continue;
               } else if (request.isStop()) {
                  queue.notifyNoWait();
                  break;
               } else if (request.isPause()) {
                  queue.notifyNoWait();
                  request.pause();
                  continue;
               }
               if (currentOffset + request.length() > maxFileSize) {
                  // switch to next file
                  logFile.close();
                  compactor.completeFile(logFile.fileId);
                  currentOffset = 0;
                  logFile = fileProvider.getFileForLog();
                  log.debug("Appending records to " + logFile.fileId);
               }
               EntryRecord.writeEntry(logFile.fileChannel, request.getSerializedKey(), request.getSerializedMetadata(), request.getSerializedValue(), nextSeqId(), request.getExpiration());
               int offset = request.getSerializedValue() == null ? ~currentOffset : currentOffset;
               temporaryTable.set(request.getKey(), logFile.fileId, offset);
               IndexRequest indexRequest = new IndexRequest(request.getKey(), raw(request.getSerializedKey()),
                     logFile.fileId, offset, request.length());
               request.setIndexRequest(indexRequest);
               indexQueue.put(indexRequest);
               currentOffset += request.length();
            } else {
               if (syncWrites) {
                  logFile.fileChannel.force(false);
               }
               queue.notifyAndWait();
            }
         }
      } catch (Exception e) {
         queue.notifyError();
         throw new RuntimeException(e);
      }
   }

   private byte[] raw(ByteBuffer buffer) {
      if (buffer.getBuf().length == buffer.getLength()) {
         return buffer.getBuf();
      } else {
         byte[] bytes = new byte[buffer.getLength()];
         System.arraycopy(buffer.getBuf(), buffer.getOffset(), bytes, 0, buffer.getLength());
         return bytes;
      }
   }

   private final long nextSeqId() {
      return seqId++;
   }

   public void stopOperations() throws InterruptedException {
      queue.pushAndWait(LogRequest.stopRequest());
      this.join();
   }
}
