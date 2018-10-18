package org.infinispan.notifications.cachemanagerlistener;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listenable;
import org.infinispan.remoting.transport.Address;

/**
 * Notifications for the cache manager
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
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
    * Returns whether there is at least one listener regitstered for the given annotation
    * @param annotationClass annotation to test for
    * @return true if there is a listener mapped to the annotation, otherwise false
    */
   boolean hasListener(Class<? extends Annotation> annotationClass);
}
