package org.infinispan.server.hotrod;

import java.util.Arrays;

/**
 * The various Hot Rod versions
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public enum HotRodVersion {
   UNKNOWN(0, 0),
   HOTROD_20(2, 0), // since 7.0
   HOTROD_21(2, 1), // since 7.1
   HOTROD_22(2, 2), // since 8.0
   HOTROD_23(2, 3), // since 8.0
   HOTROD_24(2, 4), // since 8.1
   HOTROD_25(2, 5), // since 8.2
   HOTROD_26(2, 6), // since 9.0
   HOTROD_27(2, 7), // since 9.2
   HOTROD_28(2, 8), // since 9.3
   HOTROD_29(2, 9), // since 9.4
   HOTROD_30(3, 0), // since 10.0
   HOTROD_31(3, 1), // since 12.0
   HOTROD_40(4, 0), // since 14.0
   ;

   private final int major;
   private final int minor;
   private final byte version;
   private final String text;

   HotRodVersion(int major, int minor) {
      this.major = major;
      this.minor = minor;
      this.version = (byte) (major * 10 + minor);
      this.text = version > 0 ? String.format("HOTROD/%d.%d", major, minor) : "UNKNOWN";
   }

   public byte getVersion() {
      return version;
   }

   /**
    * Checks whether the supplied version is older than the version represented by this object
    * @param version a Hot Rod version in its wire representation
    * @return true if version is older than this
    */
   public boolean isOlder(byte version) {
      return this.version > version;
   }

   /**
    * Checks whether the supplied version is equal or greater than the version represented by this object
    * @param version a Hot Rod version in its wire representation
    * @return true if version is equal or greater than this
    */
   public boolean isAtLeast(byte version) {
      return this.version <= version;
   }

   public String toString() {
      return text;
   }

   public static final HotRodVersion LATEST;
   private static final HotRodVersion[] VERSIONS = new HotRodVersion[256];

   static {
      LATEST = values()[values().length - 1];
      Arrays.fill(VERSIONS, UNKNOWN);
      for(HotRodVersion version : values()) {
         VERSIONS[version.version] = version;
      }
   }

   public static HotRodVersion forVersion(byte version) {
      return VERSIONS[version];
   }

   public static VersionedEncoder getEncoder(byte version) {
      if (HotRodVersion.HOTROD_40.isAtLeast(version)) {
         return Encoder4x.instance();
      }
      return Encoder2x.instance();
   }
}
