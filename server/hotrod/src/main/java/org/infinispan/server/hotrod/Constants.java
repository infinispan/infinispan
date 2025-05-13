package org.infinispan.server.hotrod;

/**
 * Constant values
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Constants {

   private Constants() {
   }

   public static final byte MAGIC_REQ = (byte) 0xA0;
   public static final short MAGIC_RES = 0xA1;
   public static final byte DEFAULT_CONSISTENT_HASH_VERSION_1x = 2;
   public static final byte DEFAULT_CONSISTENT_HASH_VERSION = 3;

   public static final byte INTELLIGENCE_BASIC = 0x01;
   public static final byte INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   public static final byte INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;

   public static final byte INFINITE_LIFESPAN = 0x01;
   public static final byte INFINITE_MAXIDLE = 0x02;

   public static final int DEFAULT_TOPOLOGY_ID = -1;
}
