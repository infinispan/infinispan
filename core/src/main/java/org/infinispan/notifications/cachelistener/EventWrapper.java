package org.infinispan.notifications.cachelistener;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.notifications.cachelistener.event.Event;

/**
 * Simple wrapper that keeps the original key along with the converted event and command.  The original key is required
 * for things such as key tracking. The command may be null as an event may have originated without a command.
 *
 * @author wburns
 * @since 9.0
 */
public class EventWrapper<K, V, E extends Event<K, V>> {
   private E event;
   private final K key;
   private final FlagAffectedCommand command;

   public EventWrapper(K key, E event, FlagAffectedCommand command1) {
      this.event = event;
      this.key = key;
      this.command = command1;
   }

   public E getEvent() {
      return event;
   }

   public void setEvent(E event) {
      this.event = event;
   }

   public K getKey() {
      return key;
   }

   public FlagAffectedCommand getCommand() {
      return command;
   }
}
