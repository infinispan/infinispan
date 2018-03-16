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

   static final public byte MAGIC_REQ = (byte) 0xA0;
   static final public short MAGIC_RES = 0xA1;
   static final public byte DEFAULT_CONSISTENT_HASH_VERSION_1x = 2;
   static final public byte DEFAULT_CONSISTENT_HASH_VERSION = 3;

   static final public byte INTELLIGENCE_BASIC = 0x01;
   static final public byte INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   static final public byte INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;

   static final public byte INFINITE_LIFESPAN = 0x01;
   static final public byte INFINITE_MAXIDLE = 0x02;

   static final public int DEFAULT_TOPOLOGY_ID = -1;
}
