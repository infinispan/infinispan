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
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.processors.AsyncProcessor;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class LogAppender implements Consumer<LogRequest>, Function<LogRequest, Publisher<Object>> {
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

   // These variables are only ever read from the provided executor and rxjava guarantees visibility
   // to it so they don't need to be volatile or synchronized
   private FlowableProcessor<Object> delay;
   private int currentOffset = 0;
   private long seqId = 0;
   private int receivedCount = 0;
   private final List<LogRequest> toSyncLogRequests;
   private FileProvider.Log logFile;

   // This is volatile as it can be read from different threads when submitting
   private volatile FlowableProcessor<LogRequest> flowableProcessor;

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

      this.delay = AsyncProcessor.create();
      delay.onComplete();
   }

   public synchronized void start(Executor executor) {
      assert flowableProcessor == null;
      // Need to be serialized in case if we receive requests from concurrent threads
      flowableProcessor = UnicastProcessor.<LogRequest>create().toSerialized();
      if (syncWrites) {
         // When using sync writes we may require doing a timeout flush. When this occurs it
         // is done in a different invocation chain and thus we must prevent it from happening
         // with a concurrent request
         executor = new LimitedExecutor("sifs-log-appender", executor, 1);
      }
      Scheduler scheduler = Schedulers.from(executor);

      Flowable<LogRequest> flowable = syncWrites ?
            flowableProcessor.doOnNext(lr -> {
               // Write requests must be synced - so keep track of count to compare later
               if (lr.getKey() != null) {
                  submittedCount.incrementAndGet();
               }
            }) : flowableProcessor;

      Flowable<GroupedFlowable<Boolean, LogRequest>> groupedFlowable = flowable
            // Make sure everything is observed on the executor.
            // This way it can resume and suspend itself as needed.
            .observeOn(scheduler)
            .groupBy(lr -> lr.isResume() || lr.isPause());
      groupedFlowable.subscribe(gf -> {
         if (gf.getKey()) {
            gf.subscribe(this);
         } else {
            gf.delay(this)
               .subscribe(this);
         }
      });
   }

   public synchronized void stop() {
      assert flowableProcessor != null;
      flowableProcessor.onComplete();
      flowableProcessor = null;
   }

   /**
    * Clears all the log entries returning a stage when the completion is done. Note that after the clear is complete
    * this appender will also be paused. To resume it callers must ensure they invoke {@link #resume()} to restart
    * the appender
    * @return a stage that when complete the log will be cleared and this appender is paused
    */
   public CompletionStage<Void> clearAndPause() {
      LogRequest clearRequest = LogRequest.clearRequest();
      flowableProcessor.onNext(clearRequest);
      return clearRequest;
   }

   public CompletionStage<Void> pause() {
      LogRequest pauseRequest = LogRequest.pauseRequest();
      flowableProcessor.onNext(pauseRequest);
      return pauseRequest;
   }

   public CompletionStage<Void> resume() {
      LogRequest resumeRequest = LogRequest.resumeRequest();
      flowableProcessor.onNext(resumeRequest);
      return resumeRequest;
   }

   public <K, V> CompletionStage<Void> storeRequest(int segment, MarshallableEntry<K, V> entry) {
      LogRequest storeRequest = LogRequest.storeRequest(segment, entry);
      flowableProcessor.onNext(storeRequest);
      return storeRequest;
   }

   public CompletionStage<Boolean> deleteRequest(int segment, Object key, ByteBuffer serializedKey) {
      LogRequest deleteRequest = LogRequest.deleteRequest(segment, key, serializedKey);
      flowableProcessor.onNext(deleteRequest);
      return deleteRequest.thenApply(v -> {
         try {
            return (Boolean) deleteRequest.getIndexRequest().getResult();
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      });
   }

   @Override
   public void accept(LogRequest request) {
      try {
         if (logFile == null) {
            logFile = fileProvider.getFileForLog();
            log.tracef("Appending records to %s", logFile.fileId);
         }
         if (request.isClear()) {
            logFile.close();
            completePendingLogRequests();
            currentOffset = 0;
            logFile = null;
            delay = AsyncProcessor.create();
            completeRequest(request);
            return;
         } else if (request.isPause()) {
            delay = AsyncProcessor.create();
            completeRequest(request);
            return;
         } else if (request.isResume()) {
            delay.onComplete();
            completeRequest(request);
            return;
         }
         if (currentOffset != 0 && currentOffset + request.length() > maxFileSize) {
            // switch to next file
            logFile.close();
            compactor.completeFile(logFile.fileId);
            completePendingLogRequests();
            logFile = fileProvider.getFileForLog();
            currentOffset = 0;
            log.tracef("Appending records to %s", logFile.fileId);
         }
         long seqId = nextSeqId();
         EntryRecord.writeEntry(logFile.fileChannel, request.getSerializedKey(), request.getSerializedMetadata(),
               request.getSerializedInternalMetadata(),
               request.getSerializedValue(), seqId, request.getExpiration(), request.getCreated(), request.getLastUsed());
         int offset = request.getSerializedValue() == null ? ~currentOffset : currentOffset;
         temporaryTable.set(request.getSement(), request.getKey(), logFile.fileId, offset);
         IndexRequest indexRequest = IndexRequest.update(request.getSement(), request.getKey(), raw(request.getSerializedKey()),
               logFile.fileId, offset, request.length());
         request.setIndexRequest(indexRequest);
         index.handleRequest(indexRequest);
         if (!syncWrites) {
            completeRequest(request);
         } else {
            // This cannot be null when sync writes is true
            toSyncLogRequests.add(request);
            if (submittedCount.get() == ++receivedCount || toSyncLogRequests.size() == 1000) {
               logFile.fileChannel.force(false);
               completePendingLogRequests();
            }
         }
         currentOffset += request.length();
      } catch (Exception e) {
         log.debugf("Exception encountered while processing log request %s", request);
         request.completeExceptionally(e);
      }
   }

   private void completePendingLogRequests() {
      if (toSyncLogRequests != null) {
         for (Iterator<LogRequest> iter = toSyncLogRequests.iterator(); iter.hasNext(); ) {
            LogRequest logRequest = iter.next();
            iter.remove();
            completeRequest(logRequest);
         }
      }
   }

   @Override
   public Publisher<Object> apply(LogRequest logRequest) throws Throwable {
      return delay;
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
