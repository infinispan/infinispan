package org.infinispan.client.hotrod.impl.protocol;

import java.util.HashMap;
import java.util.Map;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_10;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_11;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_12;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_13;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_20;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_21;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_22;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_23;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_24;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION_25;

/**
 * Code factory.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class CodecFactory {
   private static final Map<String, Codec> codecMap;

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

   static {
      codecMap = new HashMap<String, Codec>();
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
   }

   public static boolean isVersionDefined(String version) {
      return codecMap.containsKey(version);
   }

   public static Codec getCodec(String version) {
      if (codecMap.containsKey(version))
         return codecMap.get(version);
      else
         throw new IllegalArgumentException("Invalid Hot Rod protocol version");
   }

}
