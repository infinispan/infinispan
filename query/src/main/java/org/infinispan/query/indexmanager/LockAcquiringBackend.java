package org.infinispan.query.indexmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transitionally backend used when we receive indexing operation to be
 * applied to the (local) IndexWriter, but the index lock is not available yet.
 * We will try again to look for the lock to be made available at each incoming
 * operation, and buffer writes for later consumption if the lock is still not
 * available.
 * Such checks are synchronized, so this will cause some backpressure.
 * The buffer containing postponed write operations is also bounded and will
 * trigger more backpressure when it's filled (although filling it should not
 * be possible as the current implementation steals the locks aggressively).
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
public class LockAcquiringBackend implements IndexingBackend {

   private static final Log log = LogFactory.getLog(LockAcquiringBackend.class, Log.class);

   /**
    * Using a system property here as I don't think we'll ever hit the limit.
    */
   private static final int MAX_QUEUE_SIZE = Integer.getInteger("org.infinispan.query.indexmanager.LockAcquiringBackend.MAX_QUEUE_SIZE", 1000);
   private final BlockingQueue<Work> bufferedWork = new ArrayBlockingQueue<Work>(MAX_QUEUE_SIZE);

   private final LazyInitializableBackend clusteredSwitchingBackend;

   public LockAcquiringBackend(LazyInitializableBackend clusteredSwitchingBackend) {
      this.clusteredSwitchingBackend = clusteredSwitchingBackend;
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      log.trace("Attempting backend upgrade...");
      if (clusteredSwitchingBackend.attemptUpgrade(this)) {
         log.trace("... backend upgrade succeeded.");
         clusteredSwitchingBackend.getCurrentIndexingBackend().applyWork(workList, monitor, indexManager);
      }
      else {
         log.trace("... backend upgrade postponed.");
         enqueue(new TransactionWork(workList, monitor, indexManager));
      }
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      log.trace("Attempting backend upgrade...");
      if (clusteredSwitchingBackend.attemptUpgrade(this)) {
         log.trace("... backend upgrade succeeded.");
         clusteredSwitchingBackend.getCurrentIndexingBackend().applyStreamWork(singleOperation, monitor, indexManager);
      }
      else {
         log.trace("... backend upgrade postponed.");
         enqueue(new StreamWork(singleOperation, monitor, indexManager));
      }
   }

   private void enqueue(final Work work) {
      if (log.isDebugEnabled()) {
         int remainingCapacity = bufferedWork.remainingCapacity();
         log.debug("Need to enqueue on blocking buffer, remaining capacity to saturation: " + remainingCapacity);
      }
      final boolean done = bufferedWork.offer(work);
      if (!done) {
         if (log.isDebugEnabled()) {
            log.debug("Buffer saturated: blocking");
         }
         try {
            bufferedWork.put(work);
            log.debug("Unblocked from wait on buffer");
         } catch (InterruptedException e) {
            log.interruptedWhileBufferingWork(e);
            Thread.currentThread().interrupt();
         }
      }
   }

   @Override
   public void flushAndClose(final IndexingBackend replacement) {
      if (replacement != null) {
         final ArrayList<Work> all = new ArrayList<Work>(bufferedWork.size());
         bufferedWork.drainTo(all);
         for (Work w : all) {
            w.applyTo(replacement);
         }
      }
   }

   @Override
   public boolean isMasterLocal() {
      // We're only master when owning the lock (not yet)
      return false;
   }

   private interface Work {
      public void applyTo(IndexingBackend target);
   }

   private static class StreamWork implements Work {

      private final LuceneWork singleOperation;
      private final IndexingMonitor monitor;
      private final DirectoryBasedIndexManager indexManager;

      public StreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
         this.singleOperation = singleOperation;
         this.monitor = monitor;
         this.indexManager = indexManager;
      }

      @Override
      public void applyTo(IndexingBackend target) {
         target.applyStreamWork(singleOperation, monitor, indexManager);
      }
   }

   private static class TransactionWork implements Work {

      private final List<LuceneWork> workList;
      private final IndexingMonitor monitor;
      private final DirectoryBasedIndexManager indexManager;

      public TransactionWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
         this.workList = workList;
         this.monitor = monitor;
         this.indexManager = indexManager;
      }

      @Override
      public void applyTo(IndexingBackend target) {
         target.applyWork(workList, monitor, indexManager);
      }
   }

   public String toString() {
      return "LockAcquiringBackend";
   }

}
