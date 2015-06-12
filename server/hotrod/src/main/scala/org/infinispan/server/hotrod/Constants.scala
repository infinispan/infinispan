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
   val DEFAULT_CONSISTENT_HASH_VERSION_1x: Byte = 2
   val DEFAULT_CONSISTENT_HASH_VERSION: Byte = 3

   val INTELLIGENCE_BASIC: Byte = 0x01
   val INTELLIGENCE_TOPOLOGY_AWARE: Byte = 0x02
   val INTELLIGENCE_HASH_DISTRIBUTION_AWARE: Byte = 0x03

   val INFINITE_LIFESPAN = 0x01
   val INFINITE_MAXIDLE = 0x02
}

object Constants extends Constants
