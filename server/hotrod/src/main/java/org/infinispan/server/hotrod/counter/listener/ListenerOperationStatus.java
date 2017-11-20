package org.infinispan.server.hotrod.counter.listener;

/**
 * The listener operation (add/remove) return status.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public enum ListenerOperationStatus {
   /**
    * The operation is completed and it will use (or remove) the channel.
    */
   OK_AND_CHANNEL_IN_USE,
   /**
    * The counter wasn't found.
    */
   COUNTER_NOT_FOUND,
   /**
    * The operation is completed but it won't use the channel used in the request.
    */
   OK
}
