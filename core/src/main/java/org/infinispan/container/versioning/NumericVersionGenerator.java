package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates unique numeric versions for both local and clustered environments.
 * When used on clustered caches, node information is used to guarantee versions
 * are unique cluster-wide.
 *
 * If the cache is configured to be local, the version generated is based
 * around an atomic counter. On the contrary, if the cache is clustered, the
 * generated version is composed of:
 * [view id (2 bytes)][rank (2 bytes)][version counter (4 bytes)], where rank
 * refers to the position of this node within the view.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class NumericVersionGenerator implements VersionGenerator {

   private static final Log log = LogFactory.getLog(NumericVersionGenerator.class);

   // TODO: Possibly seed version counter on capped System.currentTimeMillis, to avoid issues with clients holding to versions in between restarts
   final AtomicInteger versionCounter = new AtomicInteger();
   final AtomicLong versionPrefix = new AtomicLong();

   private Cache<?, ?> cache;
   private boolean isClustered;

   @Inject
   public void init(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @Start(priority = 11) // after Transport
   public void start() {
      cache.getCacheManager().addListener(new RankCalculator());
      isClustered = cache.getCacheConfiguration().clustering().cacheMode().isClustered();

      if (isClustered) {
         // Use component registry to avoid keeping an instance ref simply used on start
         Transport transport = cache.getAdvancedCache().getComponentRegistry()
               .getGlobalComponentRegistry().getComponent(Transport.class);
         calculateRank(transport.getAddress(), transport.getMembers(), transport.getViewId());
      }
   }

   public NumericVersionGenerator clustered(boolean clustered) {
      isClustered = clustered;
      return this;
   }

   @Override
   public IncrementableEntryVersion generateNew() {
      long counter = versionCounter.incrementAndGet();
      return createNumericVersion(counter);

   }

   private IncrementableEntryVersion createNumericVersion(long counter) {
      // Version counter occupies the least significant 4 bytes of the version
      return isClustered
            ? new NumericVersion(versionPrefix.get() | counter)
            : new NumericVersion(counter);
   }

   @Override
   public IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      if (initialVersion instanceof NumericVersion) {
         NumericVersion old = (NumericVersion) initialVersion;
         long counter = old.getVersion() + 1;
         return createNumericVersion(counter);
      }

      throw log.unexpectedInitialVersion(initialVersion.getClass().getName());
   }

   long calculateRank(Address address, List<Address> members, long viewId) {
      long rank = findAddressRank(address, members, 1);
      // Version is composed of: <view id (2 bytes)><rank (2 bytes)><version counter (4 bytes)>
      // View id and rank form the prefix which is updated on a view change.
      long newVersionPrefix = (viewId << 48) | (rank << 32);
      // TODO: Deal with compareAndSet failures?
      versionPrefix.compareAndSet(versionPrefix.get(), newVersionPrefix);
      return versionPrefix.get();
   }

   void resetCounter() {
      versionCounter.set(0);
   }

   private int findAddressRank(Address address, List<Address> members, int rank) {
      if (address.equals(members.get(0))) return rank;
      else return findAddressRank(address, members.subList(1, members.size()), rank + 1);
   }

   @Listener
   public class RankCalculator {

      @ViewChanged
      @SuppressWarnings("unused")
      public void calculateRank(ViewChangedEvent e) {
         long rank = NumericVersionGenerator.this
               .calculateRank(e.getLocalAddress(), e.getNewMembers(), e.getViewId());
         if (log.isTraceEnabled())
            log.tracef("Calculated rank based on view %s and result was %d", e, rank);
      }

   }

}
