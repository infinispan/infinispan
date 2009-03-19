package org.horizon.notifications.cachemanagerlistener;

import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.notifications.Listenable;
import org.horizon.remoting.transport.Address;

import java.util.List;

/**
 * Notifications for the cache manager
 *
 * @author Manik Surtani
 * @since 1.0
 */
@NonVolatile
@Scope(Scopes.GLOBAL)
public interface CacheManagerNotifier extends Listenable {
   /**
    * Notifies all registered listeners of a viewChange event.  Note that viewChange notifications are ALWAYS sent
    * immediately.
    */
   void notifyViewChange(List<Address> members, Address myAddress);

   void notifyCacheStarted(String cacheName);

   void notifyCacheStopped(String cacheName);
}
