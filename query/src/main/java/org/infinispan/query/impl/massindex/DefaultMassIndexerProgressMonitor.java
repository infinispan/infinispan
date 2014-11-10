package org.infinispan.query.impl.massindex;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.infinispan.query.logging.Log;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class DefaultMassIndexerProgressMonitor implements MassIndexerProgressMonitor {

   private static final Log log = LogFactory.getLog(DefaultMassIndexerProgressMonitor.class, Log.class);
   private final AtomicLong documentsDoneCounter = new AtomicLong();
   private volatile long startTime;
   private final int logAfterNumberOfDocuments;
   private final TimeService timeService;

   /**
    * Logs progress of indexing job every 50 documents written.
    */
   public DefaultMassIndexerProgressMonitor(TimeService timeService) {
      this(50, timeService);
   }

   /**
    * Logs progress of indexing job every <code>logAfterNumberOfDocuments</code>
    * documents written.
    * 
    * @param logAfterNumberOfDocuments
    *           log each time the specified number of documents has been added
    */
   public DefaultMassIndexerProgressMonitor(int logAfterNumberOfDocuments, TimeService timeService) {
      this.logAfterNumberOfDocuments = logAfterNumberOfDocuments;
      this.timeService = timeService;
   }

   public void entitiesLoaded(int size) {
      // not used
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

   public void documentsBuilt(int number) {
      //not used
   }

   public void addToTotalCount(long count) {
      //not known
   }

   public void indexingCompleted() {
      log.indexingEntitiesCompleted(documentsDoneCounter.get());
   }

   protected int getStatusMessagePeriod() {
      return logAfterNumberOfDocuments;
   }

   protected void printStatusMessage(long startTime, long doneCount) {
      log.indexingDocumentsCompleted(doneCount, timeService.timeDuration(startTime, TimeUnit.MILLISECONDS));
   }

}
