package org.infinispan.server.hotrod

import java.util.concurrent.TimeUnit

import scala.annotation.switch

/**
 * @author gustavonalle
 * @since 8.0
 */
class TimeUnitValue(val code: Byte) extends AnyVal {

   def toJavaTimeUnit(header: HotRodHeader) = (code: @switch) match {
      case 0x00 => TimeUnit.SECONDS
      case 0x01 => TimeUnit.MILLISECONDS
      case 0x02 => TimeUnit.NANOSECONDS
      case 0x03 => TimeUnit.MICROSECONDS
      case 0x04 => TimeUnit.MINUTES
      case 0x05 => TimeUnit.HOURS
      case 0x06 => TimeUnit.DAYS
      case _ => throw new RequestParsingException(s"Invalid Time Unit code '$code'", header.version, header.messageId)
   }

   def isDefault = code == 0x07

   def isInfinite = code == 0x08
}

object TimeUnitValue {
   def SECONDS = new TimeUnitValue(0x00)

   def apply(code: Byte) = new TimeUnitValue(code)

   def decodePair(timeUnitValues: Byte): (TimeUnitValue, TimeUnitValue) =
      (TimeUnitValue(((timeUnitValues & 0xf0) >> 4).toByte), TimeUnitValue((timeUnitValues & 0x0f).toByte))

}
