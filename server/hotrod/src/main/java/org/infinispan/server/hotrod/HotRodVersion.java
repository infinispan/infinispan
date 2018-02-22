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
   HOTROD_10(1, 0),
   HOTROD_11(1, 1),
   HOTROD_12(1, 2),
   HOTROD_13(1, 3),
   HOTROD_20(2, 0),
   HOTROD_21(2, 1),
   HOTROD_22(2, 2),
   HOTROD_23(2, 3),
   HOTROD_24(2, 4),
   HOTROD_25(2, 5),
   HOTROD_26(2, 6),
   HOTROD_27(2, 7),
   HOTROD_28(2, 8),
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
   private static final HotRodVersion[] versions = new HotRodVersion[256];

   static {
      LATEST = values()[values().length - 1];
      Arrays.fill(versions, UNKNOWN);
      for(HotRodVersion version : values()) {
         versions[version.version] = version;
      }
   }

   public static HotRodVersion forVersion(byte version) {
      return versions[version];
   }

   public static VersionedDecoder getDecoder(byte version) {
      if (version < 20) {
         return new Decoder10();
      } else {
         return new Decoder2x();
      }
   }

   public static VersionedEncoder getEncoder(byte version) {
      if (version >= 20) {
         return new Encoder2x();
      } else if (version == 10) {
         return new AbstractEncoder1x() {};
      } else if (version < 20) {
         return new AbstractTopologyAwareEncoder1x() {};
      } else {
         return new Encoder2x();
      }
   }
}
