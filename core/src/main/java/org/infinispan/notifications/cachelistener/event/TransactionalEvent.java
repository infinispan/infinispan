package org.infinispan.notifications.cachelistener.event;

import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * An event subtype that includes a transaction context - if one exists - as well as a boolean as to whether the call
 * originated locally or remotely.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface TransactionalEvent<K, V> extends Event<K, V> {
   /**
    * @return the Transaction associated with the current call.  May be null if the current call is outside the scope of
    *         a transaction.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * @return true if the call originated on the local cache instance; false if originated from a remote one.
    */
   boolean isOriginLocal();
}
