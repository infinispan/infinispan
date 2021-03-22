package org.infinispan.api.common.events.cache;

import org.infinispan.api.common.events.container.ContainerEvent;

/**
 * @since 14.0
 **/
public interface CacheStartEvent extends ContainerEvent {
   String cacheName();
}
