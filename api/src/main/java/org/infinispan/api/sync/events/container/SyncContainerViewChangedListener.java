package org.infinispan.api.sync.events.container;

import org.infinispan.api.common.events.container.ViewChangeEvent;

@FunctionalInterface
public interface SyncContainerViewChangedListener {
   void onViewChanged(ViewChangeEvent event);
}
