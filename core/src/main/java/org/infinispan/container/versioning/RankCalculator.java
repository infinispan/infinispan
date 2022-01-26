package org.infinispan.container.versioning;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Compute the version prefix to be used by {@link NumericVersionGenerator} in clustered caches.
 *
 * @since 14.0
 */
@Scope(Scopes.GLOBAL)
@Listener
public class RankCalculator {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   final AtomicLong versionPrefix = new AtomicLong();

   @Inject CacheManagerNotifier cacheManagerNotifier;
   @Inject Transport transport;

   @Start
   void start() {
      if (transport != null) {
         cacheManagerNotifier.addListener(this);

         updateRank(transport.getAddress(), transport.getMembers(), transport.getViewId());
      }
   }

   @Stop
   void stop() {
      if (transport != null) {
         cacheManagerNotifier.removeListener(this);
      }
   }

   @ViewChanged
   public void updateRank(ViewChangedEvent e) {
      long rank = updateRank(e.getLocalAddress(), e.getNewMembers(), e.getViewId());
      if (log.isTraceEnabled())
         log.tracef("Calculated rank based on view %s and result was %d", e, rank);
   }

   public long getVersionPrefix() {
      return versionPrefix.get();
   }

   private long updateRank(Address address, List<Address> members, long viewId) {
      long rank = members.indexOf(address) + 1;
      // Version is composed of: <view id (2 bytes)><rank (2 bytes)><version counter (4 bytes)>
      // View id and rank form the prefix which is updated on a view change.
      long newVersionPrefix = (viewId << 48) | (rank << 32);
      versionPrefix.set(newVersionPrefix);
      return versionPrefix.get();
   }
}
