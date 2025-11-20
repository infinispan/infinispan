package org.infinispan.notifications.cachemanagerlistener;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.notifications.Listenable;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * Notifications for the cache manager
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheManagerNotifier extends Listenable {
   /**
    * Notifies all registered listeners of a viewChange event.  Note that viewChange notifications are ALWAYS sent
    * immediately.
    */
   CompletionStage<Void> notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId);

   CompletionStage<Void> notifyCacheStarted(String cacheName);

   CompletionStage<Void> notifyCacheStopped(String cacheName);

   CompletionStage<Void> notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, List<List<Address>> subgroupsMerged);

   /**
    * Notifies all registered listeners of a configurationChange event.
    *
    * @param eventType   the type of event (CREATE or REMOVE)
    * @param entityType  the type of configuration that has changed (e.g. cache, counter, ...)
    * @param entityName  the name of the configuration item that has been changed
    * @param entityValue the value of the configuration item as a map
    * @return a {@link CompletionStage} which completes when the notification has been sent.
    */
   CompletionStage<Void> notifyConfigurationChanged(ConfigurationChangedEvent.EventType eventType, String entityType, String entityName, Map<String, Object> entityValue);

   /**
    * Returns whether there is at least one listener registered for the given annotation
    *
    * @param annotationClass annotation to test for
    * @return true if there is a listener mapped to the annotation, otherwise false
    */
   boolean hasListener(Class<? extends Annotation> annotationClass);

   /**
    * Notifies all registered listeners of a sites view change event
    */
   CompletionStage<Void> notifyCrossSiteViewChanged(Collection<String> siteView, Collection<String> sitesUp, Collection<String> sitesDown);
}
