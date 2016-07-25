package org.infinispan.server.hotrod;

/**
 * @author Galder Zamarreño
 */
public enum ProtocolFlag {
   NoFlag(0),
   ForceReturnPreviousValue(1),
   DefaultLifespan(1 << 1),
   DefaultMaxIdle(1 << 2),
   SkipCacheLoader(1 << 3),
   SkipIndexing(1 << 4);

   private final byte value;

   ProtocolFlag(int value) {
      this.value = (byte) value;
   }

   public byte getValue() {
      return value;
   }
}
