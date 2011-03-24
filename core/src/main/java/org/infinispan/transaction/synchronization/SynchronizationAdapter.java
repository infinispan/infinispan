package org.infinispan.transaction.synchronization;

import org.infinispan.CacheException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.xa.XAException;

/**
 * {@link Synchronization} implementation for integrating with the TM.
 * See <a href="https://issues.jboss.org/browse/ISPN-888">ISPN-888</a> for more information on this.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class SynchronizationAdapter implements Synchronization {

   private static Log log = LogFactory.getLog(SynchronizationAdapter.class);

   private final LocalTransaction localTransaction;
   private final TransactionCoordinator txCoordinator;

   public SynchronizationAdapter(LocalTransaction localTransaction, TransactionCoordinator txCoordinator) {
      this.localTransaction = localTransaction;
      this.txCoordinator = txCoordinator;
   }

   @Override
   public void beforeCompletion() {
      if (log.isTraceEnabled()) {
         log.trace("beforeCompletion called for %s", localTransaction);
      }
      try {
         txCoordinator.prepare(localTransaction);
      } catch (XAException e) {
         throw new CacheException("Could not prepare. ", e);//todo shall we just swallow this exception?
      }
   }

   @Override
   public void afterCompletion(int status) {
      if (log.isTraceEnabled()) {
         log.trace("afterCompletion(%s) called for %s.", status, localTransaction);
      }
      if (status == Status.STATUS_COMMITTED) {
         try {
            txCoordinator.commit(localTransaction, false);
         } catch (XAException e) {
            throw new CacheException("Could not commit.", e);
         }
      } else if (status == Status.STATUS_ROLLEDBACK) {
         try {
            txCoordinator.rollback(localTransaction);
         } catch (XAException e) {
            throw new CacheException("Could not commit.", e);
         }
      } else {
         throw new IllegalArgumentException("Unknown status: " + status);
      }
   }
}
