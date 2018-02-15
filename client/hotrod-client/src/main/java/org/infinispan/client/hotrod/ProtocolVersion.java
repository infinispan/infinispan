package org.infinispan.client.hotrod;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enumeration of supported Hot Rod client protocol versions.
 *
 * @author Radoslav Husar
 * @since 9.0
 */
public enum ProtocolVersion {

   // These need to go in order of lowest version is first - this way compareTo works for versions
   PROTOCOL_VERSION_10(1, 0),
   PROTOCOL_VERSION_11(1, 1),
   PROTOCOL_VERSION_12(1, 2),
   PROTOCOL_VERSION_13(1, 3),
   PROTOCOL_VERSION_20(2, 0),
   PROTOCOL_VERSION_21(2, 1),
   PROTOCOL_VERSION_22(2, 2),
   PROTOCOL_VERSION_23(2, 3),
   PROTOCOL_VERSION_24(2, 4),
   PROTOCOL_VERSION_25(2, 5),
   PROTOCOL_VERSION_26(2, 6),
   PROTOCOL_VERSION_27(2, 7),
   PROTOCOL_VERSION_28(2, 8),
   // New versions go above this line to satisfy compareTo of enum working for versions
   ;

   public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION;

   static {
      ProtocolVersion[] versions = values();
      DEFAULT_PROTOCOL_VERSION = versions[versions.length - 1];
   }

   private final String version;

   private static final Map<String, ProtocolVersion> versions = EnumSet.allOf(ProtocolVersion.class).stream().collect(Collectors.toMap(ProtocolVersion::toString, Function.identity()));

   ProtocolVersion(int major, int minor) {
      version = String.format(Locale.ROOT, "%d.%d", major, minor);
   }

   @Override
   public String toString() {
      return version;
   }

   public static ProtocolVersion parseVersion(String version) {
      return versions.get(version);
   }
}
