package org.infinispan.container.versioning.irac;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.topology.CacheTopology;

/**
 * Default implementation of {@link IracVersionGenerator}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracVersionGenerator implements IracVersionGenerator {

   private final Map<Integer, Map<String, TopologyIracVersion>> segmentVersion;
   private final Map<Object, IracMetadata> tombstone;
   @Inject Transport transport;
   private String localSite;
   private volatile int topologyId;

   public DefaultIracVersionGenerator() {
      this.segmentVersion = new ConcurrentHashMap<>();
      this.tombstone = new ConcurrentHashMap<>();
   }

   @Start
   @Override
   public void start() {
      transport.checkCrossSiteAvailable();
      localSite = transport.localSiteName();
   }

   @Override
   public void stop() {
      //no-op
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      Map<String, TopologyIracVersion> v = segmentVersion.compute(segment, this::generateNewVectorFunction);
      return new IracMetadata(localSite, new IracEntryVersion(v));
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      segmentVersion.merge(segment, remoteVersion.toMap(), this::mergeVectorsFunction);
   }

   @Override
   public void onTopologyChange(CacheTopology newTopology) {
      topologyId = newTopology.getTopologyId();
   }

   @Override
   public void storeTombstone(Object key, IracMetadata metadata) {
      tombstone.put(key, metadata);
   }

   @Override
   public void storeTombstoneIfAbsent(Object key, IracMetadata metadata) {
      if (metadata == null) {
         return;
      }
      tombstone.putIfAbsent(key, metadata);
   }

   @Override
   public IracMetadata getTombstone(Object key) {
      return tombstone.get(key);
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      if (iracMetadata == null) {
         return;
      }
      tombstone.remove(key, iracMetadata);
   }

   @Override
   public void removeTombstone(Object key) {
      tombstone.remove(key);
   }

   private Map<String, TopologyIracVersion> generateNewVectorFunction(Integer s,
         Map<String, TopologyIracVersion> versions) {
      if (versions == null) {
         return Collections.singletonMap(localSite, TopologyIracVersion.newVersion(topologyId));
      } else {
         Map<String, TopologyIracVersion> copy = new HashMap<>(versions);
         copy.compute(localSite, this::incrementVersionFunction);
         return copy;
      }
   }

   private TopologyIracVersion incrementVersionFunction(String site, TopologyIracVersion version) {
      return version == null ? TopologyIracVersion.newVersion(topologyId) : version.increment(topologyId);
   }

   private Map<String, TopologyIracVersion> mergeVectorsFunction(Map<String, TopologyIracVersion> v1,
         Map<String, TopologyIracVersion> v2) {
      if (v1 == null) {
         return v2;
      } else {
         Map<String, TopologyIracVersion> copy = new HashMap<>(v1);
         for (Map.Entry<String, TopologyIracVersion> entry : v2.entrySet()) {
            copy.merge(entry.getKey(), entry.getValue(), TopologyIracVersion::max);
         }
         return copy;
      }
   }
}
