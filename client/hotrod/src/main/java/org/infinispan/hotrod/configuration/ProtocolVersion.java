package org.infinispan.hotrod.configuration;

import java.util.Locale;

/**
 * Enumeration of supported Hot Rod client protocol VERSIONS.
 *
 * @since 14.0
 */
public enum ProtocolVersion {

   // These need to go in order: lowest version is first - this way compareTo works for VERSIONS
   PROTOCOL_VERSION_40(4, 0),
   // New VERSIONS go above this line to satisfy compareTo of enum working for VERSIONS

   // The version here doesn't matter as long as it is >= 3.0. It must be the LAST version
   PROTOCOL_VERSION_AUTO(4, 0),
   ;

   private static final ProtocolVersion[] VERSIONS = values();

   public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = PROTOCOL_VERSION_AUTO;
   public static final ProtocolVersion HIGHEST_PROTOCOL_VERSION = VERSIONS[VERSIONS.length - 2];

   private final String textVersion;
   private final int version;

   ProtocolVersion(int major, int minor) {
      assert minor < 10;
      this.textVersion = String.format(Locale.ROOT, "%d.%d", major, minor);
      this.version = major * 10 + minor;
   }

   @Override
   public String toString() {
      return textVersion;
   }

   public int getVersion() {
      return version;
   }

   public static ProtocolVersion parseVersion(String version) {
      for (ProtocolVersion v : VERSIONS) {
         if (v.textVersion.equals(version))
            return v;
      }
      throw new IllegalArgumentException("Illegal version " + version);
   }

   public static ProtocolVersion getBestVersion(int version) {
      // We skip the last version (auto)
      for (int i = VERSIONS.length - 2; i >= 0; i--) {
         if (version >= VERSIONS[i].version)
            return VERSIONS[i];
      }
      throw new IllegalArgumentException("Illegal version " + version);
   }
}
