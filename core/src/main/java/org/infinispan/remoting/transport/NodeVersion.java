package org.infinispan.remoting.transport;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Version;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A DTO record used to represent the basic major, minor and patch values of an instance's {@link Version} over the wire.
 * @since 16.0
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.NODE_VERSION)
public record NodeVersion(byte major, byte minor, byte patch) implements Comparable<NodeVersion> {

   public static final NodeVersion INSTANCE;

   static {
      byte major = Byte.parseByte(Version.getMajor());
      byte minor = Byte.parseByte(Version.getMinor());
      byte patch = Byte.parseByte(Version.getPatch());
      INSTANCE = new NodeVersion(major, minor, patch);
   }

   public static NodeVersion from(String s) {
      var version = s.split("\\.");
      return new NodeVersion(Byte.parseByte(version[0]), Byte.parseByte(version[1]), Byte.parseByte(version[2]));
   }

   public boolean lessThan(NodeVersion other) {
      return compareTo(other) < 0;
   }

   @Override
   public int compareTo(NodeVersion o) {
      if (major != o.major) return major - o.major;
      if (minor != o.minor) return minor - o.minor;
      return patch - o.patch;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      NodeVersion that = (NodeVersion) o;
      return major == that.major && minor == that.minor && patch == that.patch;
   }

   @Override
   public int hashCode() {
      return Objects.hash(major, minor, patch);
   }

   @Override
   public String toString() {
      return String.format("%d.%d.%d", major, minor, patch);
   }
}
