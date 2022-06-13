package org.infinispan.client.hotrod;

import java.util.Locale;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.Codec20;
import org.infinispan.client.hotrod.impl.protocol.Codec21;
import org.infinispan.client.hotrod.impl.protocol.Codec22;
import org.infinispan.client.hotrod.impl.protocol.Codec23;
import org.infinispan.client.hotrod.impl.protocol.Codec24;
import org.infinispan.client.hotrod.impl.protocol.Codec25;
import org.infinispan.client.hotrod.impl.protocol.Codec26;
import org.infinispan.client.hotrod.impl.protocol.Codec27;
import org.infinispan.client.hotrod.impl.protocol.Codec28;
import org.infinispan.client.hotrod.impl.protocol.Codec29;
import org.infinispan.client.hotrod.impl.protocol.Codec30;
import org.infinispan.client.hotrod.impl.protocol.Codec31;
import org.infinispan.client.hotrod.impl.protocol.Codec40;

/**
 * Enumeration of supported Hot Rod client protocol VERSIONS.
 *
 * @author Radoslav Husar
 * @since 9.0
 */
public enum ProtocolVersion {

   // These need to go in order: lowest version is first - this way compareTo works for VERSIONS
   PROTOCOL_VERSION_20(2, 0, new Codec20()),
   PROTOCOL_VERSION_21(2, 1, new Codec21()),
   PROTOCOL_VERSION_22(2, 2, new Codec22()),
   PROTOCOL_VERSION_23(2, 3, new Codec23()),
   PROTOCOL_VERSION_24(2, 4, new Codec24()),
   PROTOCOL_VERSION_25(2, 5, new Codec25()),
   PROTOCOL_VERSION_26(2, 6, new Codec26()),
   PROTOCOL_VERSION_27(2, 7, new Codec27()),
   PROTOCOL_VERSION_28(2, 8, new Codec28()),
   PROTOCOL_VERSION_29(2, 9, new Codec29()),
   PROTOCOL_VERSION_30(3, 0, new Codec30()),
   PROTOCOL_VERSION_31(3, 1, new Codec31()),
   PROTOCOL_VERSION_40(4, 0, new Codec40()),
   // New VERSIONS go above this line to satisfy compareTo of enum working for VERSIONS

   // The version here doesn't matter as long as it is >= 3.0. It must be the LAST version
   PROTOCOL_VERSION_AUTO(4, 0, new Codec40()),
   ;

   private static final ProtocolVersion[] VERSIONS = values();

   public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = PROTOCOL_VERSION_AUTO;
   public static final ProtocolVersion HIGHEST_PROTOCOL_VERSION = VERSIONS[VERSIONS.length - 2];

   private final String textVersion;
   private final int version;
   private final Codec codec;



   ProtocolVersion(int major, int minor, Codec codec) {
      assert minor < 10;
      this.textVersion = String.format(Locale.ROOT, "%d.%d", major, minor);
      this.version = major * 10 + minor;
      this.codec = codec;
   }

   @Override
   public String toString() {
      return textVersion;
   }

   public int getVersion() {
      return version;
   }

   public Codec getCodec() {
      return codec;
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
      for (int i = VERSIONS.length - 2; i > 0; i--) {
         if (version >= VERSIONS[i].version)
            return VERSIONS[i];
      }
      throw new IllegalArgumentException("Illegal version " + version);
   }
}
