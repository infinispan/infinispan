package org.infinispan.server.hotrod

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

@Deprecated
object EncodedData extends Enumeration {
   type EncodedData = Value
   val Array = Value(1)
   val Byte = Value(1 << 1)
   val Boolean = Value(1 << 2)
   val Character = Value(1 << 3)
   val String = Value(1 << 4)
   val Date = Value(1 << 5)
   val Double = Value(1 << 6)
   // 1 << 7 skipped since that's the variable length marker
   val Float = Value(1 << 8)
   val Integer = Value(1 << 9)
   val Long = Value(1 << 10)
   val Map = Value(1 << 11)
   val Primitive = Value(1 << 12)
   val Serialized = Value(1 << 13)
   val Short = Value(1 << 14)
   // 1 << 15 skipped since that's the variable length marker
   val Compressed = Value(1 << 16)
   val StringBuilder = Value(1 << 17)

}

@Deprecated
class EncodedData(dataType: EncodedData, length: Long, data: Array[Byte]) {
}