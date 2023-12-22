package org.infinispan.api.sync.events.container;

import org.infinispan.api.common.events.container.CacheStopEvent;

@FunctionalInterface
public interface SyncContainerCacheStoppedListener {
   void onCacheStop(CacheStopEvent event);
}
