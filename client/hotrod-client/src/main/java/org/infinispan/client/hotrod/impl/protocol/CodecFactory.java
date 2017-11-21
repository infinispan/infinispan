package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_10;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_11;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_12;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_13;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_20;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_21;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_22;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_23;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_24;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_25;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_26;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_27;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.ProtocolVersion;

/**
 * Codec factory.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class CodecFactory {
   private static final Map<ProtocolVersion, Codec> codecMap;

   private static final Codec CODEC_10 = new Codec10();
   private static final Codec CODEC_11 = new Codec11();
   private static final Codec CODEC_12 = new Codec12();
   private static final Codec CODEC_13 = new Codec13();
   private static final Codec CODEC_20 = new Codec20();
   private static final Codec CODEC_21 = new Codec21();
   private static final Codec CODEC_22 = new Codec22();
   private static final Codec CODEC_23 = new Codec23();
   private static final Codec CODEC_24 = new Codec24();
   private static final Codec CODEC_25 = new Codec25();
   private static final Codec CODEC_26 = new Codec26();
   private static final Codec CODEC_27 = new Codec27();

   static {
      codecMap = new HashMap<>();
      codecMap.put(PROTOCOL_VERSION_10, CODEC_10);
      codecMap.put(PROTOCOL_VERSION_11, CODEC_11);
      codecMap.put(PROTOCOL_VERSION_12, CODEC_12);
      codecMap.put(PROTOCOL_VERSION_13, CODEC_13);
      codecMap.put(PROTOCOL_VERSION_20, CODEC_20);
      codecMap.put(PROTOCOL_VERSION_21, CODEC_21);
      codecMap.put(PROTOCOL_VERSION_22, CODEC_22);
      codecMap.put(PROTOCOL_VERSION_23, CODEC_23);
      codecMap.put(PROTOCOL_VERSION_24, CODEC_24);
      codecMap.put(PROTOCOL_VERSION_25, CODEC_25);
      codecMap.put(PROTOCOL_VERSION_26, CODEC_26);
      codecMap.put(PROTOCOL_VERSION_27, CODEC_27);
   }

   public static boolean isVersionDefined(String version) {
      final ProtocolVersion protocolVersion = ProtocolVersion.parseVersion(version);
      if (protocolVersion == null) {
         return false;
      } else {
         return codecMap.containsKey(protocolVersion);
      }
   }

   public static Codec getCodec(ProtocolVersion version) {
      if (codecMap.containsKey(version))
         return codecMap.get(version);
      else
         throw new IllegalArgumentException(String.format("Invalid Hot Rod protocol version '%s'", version));
   }

}
