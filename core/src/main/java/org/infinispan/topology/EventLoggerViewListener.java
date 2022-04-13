package org.infinispan.topology;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.List;
import java.util.function.Consumer;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

@Listener(sync = true)
public class EventLoggerViewListener {

   private final EventLogManager manager;
   private final Consumer<ViewChangedEvent> afterChange;

   public EventLoggerViewListener(EventLogManager manager, Consumer<ViewChangedEvent> afterChange) {
      this.manager = manager;
      this.afterChange = afterChange;
   }

   public EventLoggerViewListener(EventLogManager manager) {
      this(manager, ignore -> {});
   }

   @Merged
   @ViewChanged
   @SuppressWarnings("unused")
   public void handleViewChange(ViewChangedEvent event) {
      EventLogger eventLogger = manager.getEventLogger().scope(event.getLocalAddress());
      logNodeJoined(eventLogger, event.getNewMembers(), event.getOldMembers());
      logNodeLeft(eventLogger, event.getNewMembers(), event.getOldMembers());
      afterChange.accept(event);
   }

   private void logNodeJoined(EventLogger logger, List<Address> newMembers, List<Address> oldMembers) {
      newMembers.stream()
            .filter(address -> !oldMembers.contains(address))
            .forEach(address -> logger.info(EventLogCategory.CLUSTER, MESSAGES.nodeJoined(address)));
   }

   private void logNodeLeft(EventLogger logger, List<Address> newMembers, List<Address> oldMembers) {
      oldMembers.stream()
            .filter(address -> !newMembers.contains(address))
            .forEach(address -> logger.info(EventLogCategory.CLUSTER, MESSAGES.nodeLeft(address)));
   }
}
