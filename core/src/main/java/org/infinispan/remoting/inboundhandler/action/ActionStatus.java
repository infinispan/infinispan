package org.infinispan.remoting.inboundhandler.action;

/**
 * The status for an {@link Action}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public enum ActionStatus {
   /**
    * The action is completed successfully.
    */
   READY,
   /**
    * The action isn't completed yet.
    */
   NOT_READY,
   /**
    * The action is completed unsuccessfully.
    */
   CANCELED
}
