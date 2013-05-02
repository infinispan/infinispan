/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.query.impl.massindex;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.util.logging.impl.Log;
import org.hibernate.search.util.logging.impl.LoggerFactory;
import org.infinispan.util.TimeService;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public class DefaultMassIndexerProgressMonitor implements MassIndexerProgressMonitor {

   private static final Log log = LoggerFactory.make();
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
