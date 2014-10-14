package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.notifications.cachelistener.event.Event;

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

   private final Event.Type type;
   private final boolean retried;
   private final boolean pre;


   public EventType(boolean retried, boolean pre, Event.Type type) {
      this.retried = retried;
      this.pre = pre;
      this.type = type;
   }

   boolean isPreEvent() { return pre; };

   boolean isRetry() {
      return retried;
   };

   public Event.Type getType() {
      return type;
   }

   boolean isCreate() {
      return type == Event.Type.CACHE_ENTRY_CREATED;
   }

   boolean isModified() {
      return type == Event.Type.CACHE_ENTRY_MODIFIED;
   }

   boolean isRemove() {
      return type == Event.Type.CACHE_ENTRY_REMOVED;
   }
}
