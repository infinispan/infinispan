package org.infinispan.query.impl.massindex;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.time.TimeService;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
public class MassIndexerProgressMonitor {

   private static final Log log = LogFactory.getLog(MassIndexerProgressMonitor.class, Log.class);

   private final AtomicLong documentsDoneCounter = new AtomicLong();
   private volatile long startTime;
   private final int logAfterNumberOfDocuments;
   private final TimeService timeService;

   /**
    * Logs progress of indexing job every 1000 documents written.
    */
   public MassIndexerProgressMonitor(TimeService timeService) {
      this(1000, timeService);
   }

   /**
    * Logs progress of indexing job every <code>logAfterNumberOfDocuments</code>
    * documents written.
    *
    * @param logAfterNumberOfDocuments
    *           log each time the specified number of documents has been added
    */
   public MassIndexerProgressMonitor(int logAfterNumberOfDocuments, TimeService timeService) {
      this.logAfterNumberOfDocuments = logAfterNumberOfDocuments;
      this.timeService = timeService;
   }

   public void documentsAdded(long increment) {
      long current = documentsDoneCounter.addAndGet(increment);
      if (current == increment) {
         startTime = timeService.time();
      }
      if (current % getStatusMessagePeriod() == 0) {
         printStatusMessage(startTime, current);
      }
   }

   public void preIndexingReloading() {
      log.preIndexingReloading();
   }

   public void indexingStarting() {
      log.indexingStarting();
   }

   public void indexingCompleted() {
      log.indexingEntitiesCompleted(documentsDoneCounter.get(),
            timeService.timeDuration(startTime, TimeUnit.MILLISECONDS));
   }

   protected int getStatusMessagePeriod() {
      return logAfterNumberOfDocuments;
   }

   protected void printStatusMessage(long startTime, long doneCount) {
      log.indexingDocumentsCompleted(doneCount, timeService.timeDuration(startTime, TimeUnit.MILLISECONDS));
   }
}
