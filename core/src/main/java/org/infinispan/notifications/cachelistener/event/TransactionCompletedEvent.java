package org.infinispan.notifications.cachelistener.event;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}.
  * Note that this event is only delivered <i>after the fact</i>, i.e., you will never see an instance of this event with
 * {@link #isPre()} being set to <code>true</code>.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface TransactionCompletedEvent<K, V> extends TransactionalEvent<K, V> {
   /**
    * @return if <code>true</code>, the transaction completed by committing successfully.  If <code>false</code>, the
    *         transaction completed with a rollback.
    */
   boolean isTransactionSuccessful();
}
