package org.infinispan.query.indexmanager;

import java.io.IOException;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.infinispan.commons.CacheException;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Used to control and override the ownership of the Lucene index lock.
 *
 * Rather than wrapping the Directory or the LockManager directly, we need to wrap the IndexManager
 * as the Directory initialization is deferred.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class IndexManagerBasedLockController implements IndexLockController {

   private static final Log log = LogFactory.getLog(IndexManagerBasedLockController.class, Log.class);

   private final DirectoryBasedIndexManager indexManager;
   private final TransactionManager tm;

   public IndexManagerBasedLockController(DirectoryBasedIndexManager indexManager, TransactionManager tm) {
      this.indexManager = indexManager;
      this.tm = tm;
   }

   @Override
   public boolean waitForAvailability() {
      final Transaction tx = suspendTxIfExists();
      try {
         boolean waitForAvailabilityInternal = waitForAvailabilityInternal();
         log.waitingForLockAcquired(waitForAvailabilityInternal);
         return waitForAvailabilityInternal;
      }
      finally {
         resumeTx(tx);
      }
   }

   /**
    * This is returning as soon as the lock is available, or after 10 seconds.
    * @return true if the lock is free at the time of returning.
    */
   private boolean waitForAvailabilityInternal() {
      final LockFactory lockFactory = getLockFactory();
      final Lock lock = lockFactory.makeLock(IndexWriter.WRITE_LOCK_NAME);
      try {
         if (! lock.isLocked()) {
            return true;
         }
         else {
            try {
               final boolean obtained = lock.obtain( 10000 );
               if (obtained) {
                  lock.close();
                  return true;
               }
            }
            catch (LockObtainFailedException lofe) {
               return false;
            }
         }
      } catch (IOException e) {
         log.error(e);
         return false;
      }
      return false;
   }

   private void resumeTx(final Transaction tx) {
      if (tx != null) {
         try {
            tm.resume(tx);
         } catch (Exception e) {
            throw new CacheException("Unable to resume suspended transaction " + tx, e);
         }
      }
   }

   private Transaction suspendTxIfExists() {
      if (tm==null) {
         return null;
      }
      try {
         Transaction tx = null;
         if ((tx = tm.getTransaction()) != null) {
            tm.suspend();
         }
         return tx;
      }
      catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   public void forceLockClear() {
      final Transaction tx = suspendTxIfExists();
      try {
         forceLockClearInternal();
      }
      finally {
         resumeTx(tx);
      }
   }

   private void forceLockClearInternal() {
      LockFactory lockFactory = getLockFactory();
      try {
         log.warn("Forcing clear of index lock");
         lockFactory.clearLock(IndexWriter.WRITE_LOCK_NAME);
      } catch (IOException e) {
         log.error(e);
      }
   }

   private LockFactory getLockFactory() {
      Directory directory = indexManager.getDirectoryProvider().getDirectory();
      return directory.getLockFactory();
   }

}
