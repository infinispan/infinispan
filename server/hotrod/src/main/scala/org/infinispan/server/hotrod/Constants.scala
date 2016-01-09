package org.infinispan.server.hotrod

/**
 * Constant values
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
trait Constants {

   val MAGIC_REQ = 0xA0
   val MAGIC_RES = 0xA1
   val VERSION_10: Byte = 10
   val VERSION_11: Byte = 11
   val VERSION_12: Byte = 12
   val VERSION_13: Byte = 13
   val VERSION_20: Byte = 20
   val VERSION_21: Byte = 21
   val VERSION_22: Byte = 22
   val VERSION_23: Byte = 23
   val VERSION_24: Byte = 24
   val VERSION_25: Byte = 25
   val DEFAULT_CONSISTENT_HASH_VERSION_1x: Byte = 2
   val DEFAULT_CONSISTENT_HASH_VERSION: Byte = 3

   val INTELLIGENCE_BASIC: Byte = 0x01
   val INTELLIGENCE_TOPOLOGY_AWARE: Byte = 0x02
   val INTELLIGENCE_HASH_DISTRIBUTION_AWARE: Byte = 0x03

   val INFINITE_LIFESPAN = 0x01
   val INFINITE_MAXIDLE = 0x02
}

object Constants extends Constants {

   def isVersion10(v: Byte): Boolean = v == VERSION_10
   def isVersion11(v: Byte): Boolean = v == VERSION_11
   def isVersion12(v: Byte): Boolean = v == VERSION_12
   def isVersion13(v: Byte): Boolean = v == VERSION_13
   def isVersion1x(v: Byte): Boolean = v >= VERSION_10 && v <= VERSION_13
   def isVersion2x(v: Byte): Boolean = v >= VERSION_20 && v <= VERSION_25
   def isVersionKnown(v: Byte): Boolean = isVersion1x(v) || isVersion2x(v)

   /**
    * Is version previous to, and not including, 2.2?
    */
   def isVersionPre22(v: Byte): Boolean = isVersion1x(v) || v == VERSION_20 || v == VERSION_21

   /**
    * Is version previous to, and not including, 2.4?
    */
   def isVersionPre24(v: Byte): Boolean = isVersion1x(v) || (v >= VERSION_20 && v <= VERSION_23)

   /**
    * Is version previous post, and not including, 2.0?
    */
   def isVersionPost20(v: Byte): Boolean = v >= VERSION_21 && v <= VERSION_25

   def isVersionPost24(v: Byte) = v > VERSION_24


}
