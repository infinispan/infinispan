package org.infinispan.notifications.cachelistener.event;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.TransactionRegistered}.
 * <p>
 * Note that this event is only delivered <i>after the fact</i>, i.e., you will never see an instance of this event with
 * {@link #isPre()} being set to <tt>true</tt>.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface TransactionRegisteredEvent<K, V> extends TransactionalEvent<K, V> {
}
