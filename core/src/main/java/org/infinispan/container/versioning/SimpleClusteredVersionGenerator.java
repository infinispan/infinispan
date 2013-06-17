package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;

/**
 * A version generator implementation for SimpleClusteredVersions
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class SimpleClusteredVersionGenerator implements VersionGenerator {
   // The current cache topology ID is recorded and used as a part of the version generated, and as such used as the
   // most significant part of a version comparison. If a version is generated based on an old cache topology and another is
   // generated based on a newer topology, the one based on the newer topology wins regardless of the version's counter.
   // See SimpleClusteredVersion for more details.
   private volatile int topologyId = -1;

   private static final SimpleClusteredVersion NON_EXISTING = new SimpleClusteredVersion(0, 0);

   private Cache<?, ?> cache;

   @Inject
   public void init(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @Start(priority = 11)
   public void start() {
      cache.addListener(new TopologyIdUpdater());
   }

   @Override
   public IncrementableEntryVersion generateNew() {
      if (topologyId == -1) {
         throw new IllegalStateException("Topology id not set yet");
      }
      return new SimpleClusteredVersion(topologyId, 1);
   }

   @Override
   public IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      if (initialVersion instanceof SimpleClusteredVersion) {
         SimpleClusteredVersion old = (SimpleClusteredVersion) initialVersion;
         return new SimpleClusteredVersion(topologyId, old.version + 1);
      } else {
         throw new IllegalArgumentException("I only know how to deal with SimpleClusteredVersions, not " + initialVersion.getClass().getName());
      }
   }

   @Override
   public IncrementableEntryVersion nonExistingVersion() {
      return NON_EXISTING;
   }

   @Listener
   public class TopologyIdUpdater {

      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
         topologyId = tce.getNewTopologyId();
      }
   }
}
