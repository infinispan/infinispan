package org.infinispan.client.hotrod.impl.protocol;

import java.util.HashMap;
import java.util.Map;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.*;

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

   static {
      codecMap = new HashMap<String, Codec>();
      codecMap.put(PROTOCOL_VERSION_10, CODEC_10);
      codecMap.put(PROTOCOL_VERSION_11, CODEC_11);
      codecMap.put(PROTOCOL_VERSION_12, CODEC_12);
      codecMap.put(PROTOCOL_VERSION_13, CODEC_13);
      codecMap.put(PROTOCOL_VERSION_20, CODEC_20);
   }

   public static Codec getCodec(String version) {
      if (codecMap.containsKey(version))
         return codecMap.get(version);
      else
         throw new IllegalArgumentException("Invalid Hot Rod protocol version");
   }

}
