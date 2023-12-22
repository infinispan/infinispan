package org.infinispan.api.sync.events.container;

import org.infinispan.api.common.events.container.CacheStartEvent;

@FunctionalInterface
public interface SyncContainerCacheStartedListener {
   void onCacheStart(CacheStartEvent event);
}
