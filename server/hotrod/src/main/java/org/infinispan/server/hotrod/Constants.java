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

   static final public short MAGIC_REQ = 0xA0;
   static final public short MAGIC_RES = 0xA1;
   static final public byte VERSION_10 = 10;
   static final public byte VERSION_11 = 11;
   static final public byte VERSION_12 = 12;
   static final public byte VERSION_13 = 13;
   static final public byte VERSION_20 = 20;
   static final public byte VERSION_21 = 21;
   static final public byte VERSION_22 = 22;
   static final public byte VERSION_23 = 23;
   static final public byte VERSION_24 = 24;
   static final public byte VERSION_25 = 25;
   static final public byte VERSION_26 = 26;
   public static final byte VERSION_27 = 27;
   static final public byte DEFAULT_CONSISTENT_HASH_VERSION_1x = 2;
   static final public byte DEFAULT_CONSISTENT_HASH_VERSION = 3;

   static final public byte INTELLIGENCE_BASIC = 0x01;
   static final public byte INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   static final public byte INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;

   static final public byte INFINITE_LIFESPAN = 0x01;
   static final public byte INFINITE_MAXIDLE = 0x02;

   static final public int DEFAULT_TOPOLOGY_ID = -1;

   static final public boolean isVersion10(byte v) {
      return v == VERSION_10;
   }

   static final public boolean isVersion11(byte v) {
      return v == VERSION_11;
   }

   static final public boolean isVersion12(byte v) {
      return v == VERSION_12;
   }

   static final public boolean isVersion13(byte v) {
      return v == VERSION_13;
   }

   static final public boolean isVersion1x(byte v) {
      return v >= VERSION_10 && v <= VERSION_13;
   }

   static final public boolean isVersion2x(byte v) {
      return v >= VERSION_20 && v <= VERSION_27;
   }

   static final public boolean isVersionKnown(byte v) {
      return isVersion1x(v) || isVersion2x(v);
   }

   /**
    * Is version previous to, and not including, 2.2?
    */
   static public boolean isVersionPre22(byte v) {
      return isVersion1x(v) || v == VERSION_20 || v == VERSION_21;
   }

   /**
    * Is version previous to, and not including, 2.4?
    */
   static public boolean isVersionPre24(byte v) {
      return isVersion1x(v) || (v >= VERSION_20 && v <= VERSION_23);
   }

   /**
    * Is version previous post, and not including, 2.0?
    */
   static public boolean isVersionPost20(byte v) {
      return v >= VERSION_21 && v <= VERSION_27;
   }

   static public boolean isVersionPost24(byte v) {
      return v > VERSION_24;
   }

   static public boolean isVersionPost25(byte v) {
      return v > VERSION_25;
   }

}
