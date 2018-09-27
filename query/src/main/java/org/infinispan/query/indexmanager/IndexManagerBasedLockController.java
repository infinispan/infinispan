package org.infinispan.query.indexmanager;

import java.io.IOException;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.infinispan.lucene.impl.DirectoryExtensions;
import org.infinispan.query.backend.TransactionHelper;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Used to control and override the ownership of the Lucene index lock.
 * <p>
 * Rather than wrapping the Directory or the LockManager directly, we need to wrap the IndexManager
 * as the Directory initialization is deferred.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class IndexManagerBasedLockController implements IndexLockController {

   private static final Log log = LogFactory.getLog(IndexManagerBasedLockController.class, Log.class);

   private final DirectoryBasedIndexManager indexManager;
   private final TransactionHelper transactionHelper;

   IndexManagerBasedLockController(DirectoryBasedIndexManager indexManager, TransactionManager tm) {
      this.indexManager = indexManager;
      this.transactionHelper = new TransactionHelper(tm);
   }

   @Override
   public boolean waitForAvailability() {
      final Transaction tx = transactionHelper.suspendTxIfExists();
      try {
         boolean waitForAvailabilityInternal = waitForAvailabilityInternal();
         log.waitingForLockAcquired(waitForAvailabilityInternal);
         return waitForAvailabilityInternal;
      } finally {
         transactionHelper.resume(tx);
      }
   }

   /**
    * This is returning as soon as the lock is available, or after 10 seconds.
    *
    * @return true if the lock is free at the time of returning.
    */
   private boolean waitForAvailabilityInternal() {
      final Directory directory = indexManager.getDirectoryProvider().getDirectory();
      try {
         Lock lock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
         lock.close();
         return true;
      } catch (LockObtainFailedException lofe) {
         return false;
      } catch (IOException e) {
         log.error(e);
         return false;
      }
   }

   @Override
   public void forceLockClear() {
      final Transaction tx = transactionHelper.suspendTxIfExists();
      try {
         forceLockClearInternal();
      } finally {
         transactionHelper.resume(tx);
      }
   }

   private void forceLockClearInternal() {
      final Directory directory = indexManager.getDirectoryProvider().getDirectory();
      log.warn("Forcing clear of index lock");
      ((DirectoryExtensions) directory).forceUnlock(IndexWriter.WRITE_LOCK_NAME);
   }

}
