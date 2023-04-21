package org.infinispan.server.resp.commands.pubsub;

import java.util.Arrays;

public final class KeyChannelUtils {

   private KeyChannelUtils() {
   }

   // Random bytes to keep listener keys separate from others. Means `resp|`.
   public static final byte[] PREFIX_CHANNEL_BYTES = new byte[]{114, 101, 115, 112, 124};

   public static byte[] keyToChannel(byte[] keyBytes) {
      byte[] result = new byte[keyBytes.length + PREFIX_CHANNEL_BYTES.length];
      System.arraycopy(PREFIX_CHANNEL_BYTES, 0, result, 0, PREFIX_CHANNEL_BYTES.length);
      System.arraycopy(keyBytes, 0, result, PREFIX_CHANNEL_BYTES.length, keyBytes.length);
      return result;
   }

   public static byte[] channelToKey(byte[] channelBytes) {
      return Arrays.copyOfRange(channelBytes, PREFIX_CHANNEL_BYTES.length, channelBytes.length);
   }
}
