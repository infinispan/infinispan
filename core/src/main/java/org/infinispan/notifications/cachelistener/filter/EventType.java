package org.infinispan.notifications.cachelistener.filter;

/**
 * Enum that provides information to allow for an event to know which type and if this event was generated due to a
 * retry usually caused by a topology change while replicating.
 *
 * @author wburns
 * @since 7.0
 */
public class EventType {
   public enum Operation {
      CREATE, REMOVE, MODIFY;
   }

   private final Operation operation;
   private final boolean retried;
   private final boolean pre;


   public EventType(boolean retried, boolean pre, Operation operation) {
      this.retried = retried;
      this.pre = pre;
      this.operation = operation;
   }

   boolean isPreEvent() { return pre; };

   boolean isRetry() {
      return retried;
   };

   boolean isCreate() {
      return operation == Operation.CREATE;
   }

   boolean isModified() {
      return operation == Operation.MODIFY;
   }

   boolean isRemove() {
      return operation == Operation.REMOVE;
   }
}
