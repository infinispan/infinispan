package org.infinispan.notifications.cachemanagerlistener;

import java.util.List;

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
   void notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId);

   void notifyCacheStarted(String cacheName);

   void notifyCacheStopped(String cacheName);

   void notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, List<List<Address>> subgroupsMerged);
}
