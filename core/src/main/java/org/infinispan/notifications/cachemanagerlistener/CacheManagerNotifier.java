package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listenable;
import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * Notifications for the cache manager
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface CacheManagerNotifier extends Listenable {
   /**
    * Notifies all registered listeners of a viewChange event.  Note that viewChange notifications are ALWAYS sent
    * immediately.
    */
   void notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, boolean requiresJoin, boolean isMerge);

   void notifyCacheStarted(String cacheName);

   void notifyCacheStopped(String cacheName);

   void notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, boolean requiresRejoin, List<List<Address>> subgroupsMerged);
}
