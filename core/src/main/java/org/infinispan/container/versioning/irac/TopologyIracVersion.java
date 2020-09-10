package org.infinispan.container.versioning.irac;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

   public static final TopologyIracVersion NO_VERSION = new TopologyIracVersion(0, 0);
   private static final Pattern PARSE_PATTERN = Pattern.compile("\\((\\d+):(\\d+)\\)");

   private final int topologyId;
   private final long version;

   private TopologyIracVersion(int topologyId, long version) {
      this.topologyId = topologyId;
      this.version = version;
   }

   @ProtoFactory
   public static TopologyIracVersion create(int topologyId, long version) {
      return topologyId == 0 && version == 0 ? NO_VERSION : new TopologyIracVersion(topologyId, version);
   }

   public static TopologyIracVersion newVersion(int currentTopologyId) {
      return new TopologyIracVersion(currentTopologyId, 1);
   }

   public static TopologyIracVersion max(TopologyIracVersion v1, TopologyIracVersion v2) {
      return v1.compareTo(v2) < 0 ? v2 : v1;
   }

   public static TopologyIracVersion fromString(String s) {
      Matcher m = PARSE_PATTERN.matcher(s);
      if (!m.find()) {
         return null;
      }
      int topology = Integer.parseInt(m.group(1));
      long version = Long.parseLong(m.group(2));
      return new TopologyIracVersion(topology, version);
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
