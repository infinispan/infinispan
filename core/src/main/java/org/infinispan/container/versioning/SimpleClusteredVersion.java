package org.infinispan.container.versioning;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import net.jcip.annotations.Immutable;

/**
 * A simple versioning scheme that is cluster-aware
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Immutable
@ProtoTypeId(ProtoStreamTypeIds.SIMPLE_CLUSTERED_VERSION)
public class SimpleClusteredVersion implements IncrementableEntryVersion {
   /**
    * The cache topology id in which it was first created.
    */
   private final int topologyId;

   private final long version;

   @ProtoFactory
   public SimpleClusteredVersion(int topologyId, long version) {
      this.version = version;
      this.topologyId = topologyId;
   }

   @ProtoField(number = 1, defaultValue = "-1")
   public int getTopologyId() {
      return topologyId;
   }

   @ProtoField(number = 2, defaultValue = "-1")
   public long getVersion() {
      return version;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof SimpleClusteredVersion) {
         SimpleClusteredVersion otherVersion = (SimpleClusteredVersion) other;

         if (topologyId > otherVersion.topologyId)
            return InequalVersionComparisonResult.AFTER;
         if (topologyId < otherVersion.topologyId)
            return InequalVersionComparisonResult.BEFORE;

         if (version > otherVersion.version)
            return InequalVersionComparisonResult.AFTER;
         if (version < otherVersion.version)
            return InequalVersionComparisonResult.BEFORE;

         return InequalVersionComparisonResult.EQUAL;
      } else {
         throw new IllegalArgumentException("I only know how to deal with SimpleClusteredVersions, not " + other.getClass().getName());
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SimpleClusteredVersion that = (SimpleClusteredVersion) o;
      return topologyId == that.topologyId &&
            version == that.version;
   }

   @Override
   public int hashCode() {
      return Objects.hash(topologyId, version);
   }

   @Override
   public String toString() {
      return "SimpleClusteredVersion{" +
            "topologyId=" + topologyId +
            ", version=" + version +
            '}';
   }
}
