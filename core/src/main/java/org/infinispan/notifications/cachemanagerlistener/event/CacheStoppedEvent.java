package org.infinispan.notifications.cachemanagerlistener.event;

/**
 * This event is passed in to any method annotated with {@link org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped}.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */
public interface CacheStoppedEvent extends Event {
   String getCacheName();
}
