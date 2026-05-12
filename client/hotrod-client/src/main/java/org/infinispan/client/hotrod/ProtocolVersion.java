package org.infinispan.client.hotrod;

import java.util.BitSet;
import java.util.Locale;
import java.util.function.Function;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.Codec30;
import org.infinispan.client.hotrod.impl.protocol.Codec31;
import org.infinispan.client.hotrod.impl.protocol.Codec40;
import org.infinispan.client.hotrod.impl.protocol.Codec41;

/**
 * Enumeration of supported Hot Rod client protocol VERSIONS.
 *
 * @author Radoslav Husar
 * @since 9.0
 */
public enum ProtocolVersion {

   // These need to go in order: lowest version is first - this way compareTo works for VERSIONS
   PROTOCOL_VERSION_30(3, 0, Codec30::new),
   PROTOCOL_VERSION_31(3, 1, Codec31::new),
   PROTOCOL_VERSION_40(4, 0, Codec40::new),
   PROTOCOL_VERSION_41(4, 1, Codec41::new),
   // New VERSIONS go above this line to satisfy compareTo of enum working for VERSIONS

   // The version here doesn't matter as long as it is >= 3.0. It must be the LAST version
   PROTOCOL_VERSION_AUTO(4, 1, "AUTO", Codec41::new),
   ;
   private static final BitSet ALL_OPS = new BitSet(0xFF);
   private static final ProtocolVersion[] VERSIONS = values();

   public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = PROTOCOL_VERSION_AUTO;
   public static final ProtocolVersion HIGHEST_PROTOCOL_VERSION = VERSIONS[VERSIONS.length - 2];
   public static final ProtocolVersion SAFE_HANDSHAKE_PROTOCOL_VERSION = PROTOCOL_VERSION_31;


   private final String textVersion;
   private final int version;
   private final Function<BitSet, Codec> codec;

   ProtocolVersion(int major, int minor, Function<BitSet, Codec> codec) {
      this(major, minor, String.format(Locale.ROOT, "%d.%d", major, minor), codec);
   }

   ProtocolVersion(int major, int minor, String name, Function<BitSet, Codec> codec) {
      assert minor < 10;
      this.textVersion = name;
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
      return codec.apply(ALL_OPS);
   }

   public Codec getCodec(BitSet supportedOps) {
      return codec.apply(supportedOps);
   }

   public static ProtocolVersion parseVersion(String version) {
      if ("AUTO".equalsIgnoreCase(version)) {
         return PROTOCOL_VERSION_AUTO;
      }
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
