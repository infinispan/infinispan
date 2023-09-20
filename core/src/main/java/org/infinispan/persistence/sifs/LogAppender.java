package org.infinispan.persistence.sifs;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class LogAppender implements Consumer<LogAppender.WriteOperation> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private final NonBlockingManager nonBlockingManager;
   private final Index index;
   private final TemporaryTable temporaryTable;
   private final Compactor compactor;
   private final FileProvider fileProvider;
   private final boolean syncWrites;
   private final int maxFileSize;
   // Used to keep track of how many log requests have been submitted. This way if the blocking thread has consumed
   // the same number of log requests it can immediately flush.
   private final AtomicInteger submittedCount = new AtomicInteger();
   // This variable is null unless sync writes are enabled. When sync writes are enabled this list holds
   // all the log requests that should be completed when the disk is ensured to be flushed
   private final List<Consumer<LogAppender>> toSyncLogRequests;

   // This buffer is used by the log appender thread to avoid allocating buffers per entry written that are smaller
   // than the header size
   private final java.nio.ByteBuffer REUSED_BUFFER = java.nio.ByteBuffer.allocate(EntryHeader.HEADER_SIZE_11_0);

   // These variables are only ever read from the provided executor and rxjava guarantees visibility
   // to them, so they don't need to be volatile or synchronized
   private int currentOffset = 0;
   private long seqId = 0;
   private int receivedCount = 0;
   private List<LogRequest> delayedLogRequests;
   private FileProvider.Log logFile;
   private long nextExpirationTime = -1;

   // This is volatile as it can be read from different threads when submitting
   private volatile FlowableProcessor<LogRequest> requestProcessor;
   // This is only accessed by the requestProcessor thread
   private FlowableProcessor<WriteOperation> writeProcessor;
   // This is only accessed by the writeProcessor thread
   private FlowableProcessor<Consumer<LogAppender>> completionProcessor;

   public LogAppender(NonBlockingManager nonBlockingManager, Index index,
                      TemporaryTable temporaryTable, Compactor compactor,
                      FileProvider fileProvider, boolean syncWrites, int maxFileSize) {
      this.nonBlockingManager = nonBlockingManager;
      this.index = index;
      this.temporaryTable = temporaryTable;
      this.compactor = compactor;
      this.fileProvider = fileProvider;
      this.syncWrites = syncWrites;
      this.maxFileSize = maxFileSize;

      this.toSyncLogRequests = syncWrites ? new ArrayList<>() : null;
   }

   public synchronized void start(Executor executor) {
      assert requestProcessor == null;

      writeProcessor = UnicastProcessor.create();
      writeProcessor.observeOn(Schedulers.from(executor))
            .subscribe(this, e -> log.warn("Exception encountered while performing write log request ", e));

      completionProcessor = UnicastProcessor.create();
      completionProcessor.observeOn(nonBlockingManager.asScheduler())
            .subscribe(this::complete, e -> log.warn("Exception encountered while performing write log request ", e));

      // Need to be serialized in case if we receive requests from concurrent threads
      requestProcessor = UnicastProcessor.<LogRequest>create().toSerialized();
      requestProcessor.subscribe(this::callerAccept,
            e -> log.warn("Exception encountered while handling log request for log appender", e), () -> {
               writeProcessor.onComplete();
               writeProcessor = null;
               completionProcessor.onComplete();
               completionProcessor = null;
               if (logFile != null) {
                  Util.close(logFile);
                  // add the current appended file - note this method will fail if it is already present, which will
                  // happen if there are some free entries
                  compactor.addFreeFile(logFile.fileId, (int) fileProvider.getFileSize(logFile.fileId), 0, nextExpirationTime);
                  logFile = null;
               }
            });
   }

   public synchronized void stop() {
      assert requestProcessor != null;
      requestProcessor.onComplete();
      requestProcessor = null;
   }

   void handleRequestCompletion(LogRequest request) {
      int offset = request.getSerializedValue() == null ? ~request.getFileOffset() : request.getFileOffset();
      temporaryTable.set(request.getSegment(), request.getKey(), request.getFile(), offset);
      IndexRequest indexRequest = IndexRequest.update(request.getSegment(), request.getKey(), request.getSerializedKey(),
            request.getSerializedValue() == null, request.getFile(), offset, request.length());
      request.setIndexRequest(indexRequest);
      index.handleRequest(indexRequest);

      completeRequest(request);
   }

   static class WriteOperation implements Consumer<LogAppender> {
      private final LogRequest logRequest;
      private final java.nio.ByteBuffer serializedKey;
      private final java.nio.ByteBuffer serializedMetadata;
      private final java.nio.ByteBuffer serializedValue;
      private final java.nio.ByteBuffer serializedInternalMetadata;

      private WriteOperation(LogRequest logRequest, java.nio.ByteBuffer serializedKey,
            java.nio.ByteBuffer serializedMetadata, java.nio.ByteBuffer serializedValue,
            java.nio.ByteBuffer serializedInternalMetadata) {
         this.logRequest = logRequest;
         this.serializedKey = serializedKey;
         this.serializedMetadata = serializedMetadata;
         this.serializedValue = serializedValue;
         this.serializedInternalMetadata = serializedInternalMetadata;
      }

      static WriteOperation fromLogRequest(LogRequest logRequest) {
         return new WriteOperation(logRequest, fromISPNByteBuffer(logRequest.getSerializedKey()),
               fromISPNByteBuffer(logRequest.getSerializedMetadata()),
               fromISPNByteBuffer(logRequest.getSerializedValue()),
               fromISPNByteBuffer(logRequest.getSerializedInternalMetadata()));
      }

      static java.nio.ByteBuffer fromISPNByteBuffer(ByteBuffer byteBuffer) {
         if (byteBuffer == null) {
            return null;
         }
         return java.nio.ByteBuffer.wrap(byteBuffer.getBuf(), byteBuffer.getOffset(), byteBuffer.getLength());
      }

      // This method isn't required, here solely to avoid allocating an additional Consumer for the non sync case
      @Override
      public void accept(LogAppender appender) throws Throwable {
         appender.handleRequestCompletion(logRequest);
      }
   }

   /**
    * Clears all the log entries returning a stage when the completion is done. Note that after the clear is complete
    * this appender will also be paused. To resume it callers must ensure they invoke {@link #resume()} to restart
    * the appender
    * @return a stage that when complete the log will be cleared and this appender is paused
    */
   public CompletionStage<Void> clearAndPause() {
      LogRequest clearRequest = LogRequest.clearRequest();
      requestProcessor.onNext(clearRequest);
      return clearRequest;
   }

   public CompletionStage<Void> pause() {
      LogRequest pauseRequest = LogRequest.pauseRequest();
      requestProcessor.onNext(pauseRequest);
      return pauseRequest;
   }

   public CompletionStage<Void> resume() {
      LogRequest resumeRequest = LogRequest.resumeRequest();
      requestProcessor.onNext(resumeRequest);
      return resumeRequest;
   }

   public <K, V> CompletionStage<Void> storeRequest(int segment, MarshallableEntry<K, V> entry) {
      LogRequest storeRequest = LogRequest.storeRequest(segment, entry);
      requestProcessor.onNext(storeRequest);
      return storeRequest;
   }

   public CompletionStage<Boolean> deleteRequest(int segment, Object key, ByteBuffer serializedKey) {
      LogRequest deleteRequest = LogRequest.deleteRequest(segment, key, serializedKey);
      requestProcessor.onNext(deleteRequest);
      return deleteRequest.thenCompose(v -> cast(deleteRequest.getIndexRequest()));
   }

   private static <I> CompletionStage<I> cast(CompletionStage stage) {
      return (CompletionStage<I>) stage;
   }

   /**
    * This method is invoked for every request sent via {@link #storeRequest(int, MarshallableEntry)},
    * {@link #deleteRequest(int, Object, ByteBuffer)} and {@link #clearAndPause()}. Note this method is only invoked
    * by one thread at any time and has visibility guaranatees as provided by rxjava.
    * @param request the log request
    */
   private void callerAccept(LogRequest request) {
      if (request.isPause()) {
         delayedLogRequests = new ArrayList<>();
         // This request is created in the same thread - so there can be no dependents
         request.complete(null);
         return;
      } else if (request.isResume()) {
         delayedLogRequests.forEach(this::sendToWriteProcessor);
         delayedLogRequests = null;
         // This request is created in the same thread - so there can be no dependents
         request.complete(null);
         return;
      } else if (request.isClear()) {
         assert delayedLogRequests == null;
         delayedLogRequests = new ArrayList<>();
      } else if (delayedLogRequests != null) {
         // We were paused - so enqueue the request for later
         delayedLogRequests.add(request);
         return;
      }

      sendToWriteProcessor(request);
   }

   private void sendToWriteProcessor(LogRequest request) {
      // Write requests must be synced - so keep track of count to compare later
      if (syncWrites && request.getKey() != null) {
         submittedCount.incrementAndGet();
      }

      writeProcessor.onNext(WriteOperation.fromLogRequest(request));
   }

   @Override
   public void accept(WriteOperation writeOperation) {
      LogRequest actualRequest = writeOperation.logRequest;
      try {
         if (logFile == null) {
            logFile = fileProvider.getFileForLog();
            log.tracef("Appending records to %s", logFile.fileId);
         }

         if (actualRequest.isClear()) {
            logFile.close();
            completePendingLogRequests();
            nextExpirationTime = -1;
            currentOffset = 0;
            logFile = null;
            completeRequest(actualRequest);
            return;
         }
         int actualLength = actualRequest.length();
         if (currentOffset != 0 && currentOffset + actualLength > maxFileSize) {
            // switch to next file
            logFile.close();
            completePendingLogRequests();

            final int fileId = logFile.fileId;
            final int offset = currentOffset;
            final long exp = nextExpirationTime;
            // Have to schedule the compaction after all other log appender operations are complete and register their
            // index updates. Then we can do a sync index call to ensure the compactor is ran after all updates are done
            completionProcessor.onNext(la -> la.index.ensureRunOnLast(() -> compactor.completeFile(fileId, offset, exp)));

            logFile = fileProvider.getFileForLog();
            nextExpirationTime = -1;
            currentOffset = 0;
            log.tracef("Appending records to %s", logFile.fileId);
         }
         long seqId = nextSeqId();
         log.tracef("Appending record to %s:%s", logFile.fileId, currentOffset);
         nextExpirationTime = ExpiryHelper.mostRecentExpirationTime(nextExpirationTime, actualRequest.getExpiration());
         EntryRecord.writeEntry(logFile.fileChannel, REUSED_BUFFER, writeOperation.serializedKey,
               writeOperation.serializedMetadata, writeOperation.serializedInternalMetadata,
               writeOperation.serializedValue, seqId, actualRequest.getExpiration(), actualRequest.getCreated(),
               actualRequest.getLastUsed());
         actualRequest.setFile(logFile.fileId);
         actualRequest.setFileOffset(currentOffset);

         if (!syncWrites) {
            completionProcessor.onNext(writeOperation);
         } else {
            // This cannot be null when sync writes is true
            toSyncLogRequests.add(la -> la.handleRequestCompletion(actualRequest));
            if (submittedCount.get() == ++receivedCount || toSyncLogRequests.size() == 1000) {
               logFile.fileChannel.force(false);
               completePendingLogRequests();
            }
         }
         currentOffset += actualLength;
      } catch (Exception e) {
         log.debugf("Exception encountered while processing log request %s", actualRequest);
         actualRequest.completeExceptionally(e);
      }
   }

   public void complete(Consumer<LogAppender> consumer) throws Throwable {
      consumer.accept(this);
   }

   /**
    * Must only be invoked by {@link #accept(WriteOperation)} method.
    */
   private void completePendingLogRequests() {
      if (toSyncLogRequests != null) {
         for (Iterator<Consumer<LogAppender>> iter = toSyncLogRequests.iterator(); iter.hasNext(); ) {
            Consumer<LogAppender> consumer = iter.next();
            iter.remove();
            completionProcessor.onNext(consumer);
         }
      }
   }

   public void setSeqId(long seqId) {
      this.seqId = seqId;
   }

   private long nextSeqId() {
      return seqId++;
   }

   private void completeRequest(CompletableFuture<Void> future) {
      nonBlockingManager.complete(future, null);
   }
}
