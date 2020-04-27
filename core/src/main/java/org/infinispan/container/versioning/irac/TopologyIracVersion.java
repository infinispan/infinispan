package org.infinispan.container.versioning.irac;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Type;

/**
 * The version stored per {@link CacheEntry} for IRAC.
 * <p>
 * It is composed by the topology Id and a version. The topology Id is increment when the topology changes and the
 * version on each update.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_SITE_VERSION)
public class TopologyIracVersion implements Comparable<TopologyIracVersion> {

   private final int topologyId;
   private final long version;

   @ProtoFactory
   public TopologyIracVersion(int topologyId, long version) {
      this.topologyId = topologyId;
      this.version = version;
   }

   public static TopologyIracVersion newVersion(int currentTopologyId) {
      return new TopologyIracVersion(currentTopologyId, 1);
   }

   public static TopologyIracVersion max(TopologyIracVersion v1, TopologyIracVersion v2) {
      return v1.compareTo(v2) < 0 ? v2 : v1;
   }

   @ProtoField(number = 1, type = Type.UINT32, defaultValue = "0")
   public int getTopologyId() {
      return topologyId;
   }

   @ProtoField(number = 2, type = Type.UINT64, defaultValue = "0")
   public long getVersion() {
      return version;
   }

   public TopologyIracVersion increment(int currentTopologyId) {
      long newVersion = currentTopologyId == topologyId ? version + 1 : 1;
      return new TopologyIracVersion(currentTopologyId, newVersion);
   }

   @Override
   public int compareTo(TopologyIracVersion other) {
      if (topologyId < other.topologyId) {
         return -1;
      } else if (topologyId > other.topologyId) {
         return 1;
      }
      return Long.compare(version, other.version);
   }

   @Override
   public String toString() {
      return '(' + Integer.toString(topologyId) + ':' + version + ')';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      TopologyIracVersion that = (TopologyIracVersion) o;
      return this.topologyId == that.topologyId &&
            this.version == that.version;
   }

   @Override
   public int hashCode() {
      int result = topologyId;
      result = 31 * result + (int) (version ^ (version >>> 32));
      return result;
   }
}
