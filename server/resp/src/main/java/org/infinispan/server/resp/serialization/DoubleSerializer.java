package org.infinispan.server.resp.serialization;

import java.nio.charset.StandardCharsets;

import org.infinispan.server.resp.ByteBufPool;

import io.netty.buffer.ByteBuf;

/**
 * Represent a double-precision floating point.
 *
 * <p>
 * The prefix is a comma character, followed by the floating point value in a human-readable form. The double representation
 * has different formats. The value can be in base-10 format with the integral and fractional parts or using scientific notation.
 * Additionally, double-precision numbers can also be infinity or NaN.
 * </p>
 *
 * @since 15.0
 * @author José Bolina
 */
final class DoubleSerializer implements ResponseSerializer<Double> {
   static final DoubleSerializer INSTANCE = new DoubleSerializer();
   private static final byte[] NAN = {'n', 'a', 'n'};
   private static final byte[] INF = {'i', 'n', 'f'};

   @Override
   public void accept(Double d, ByteBufPool alloc) {
      // RESP: ,nan\r\n
      if (d.isNaN()) {
         writeNaN(alloc);
         return;
      }

      // RESP: ,inf\r\n
      // RESP: ,-inf\r\n
      if (d.isInfinite()) {
         writeInfinite(d >= 0,alloc);
         return;
      }


      // RESP: ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
      byte[] transformed = serializeDouble(d);
      int size = 1 + transformed.length + RespConstants.CRLF.length;
      alloc.acquire(size)
            .writeByte(RespConstants.DOUBLE)
            .writeBytes(transformed)
            .writeBytes(RespConstants.CRLF);
   }

   private void writeNaN(ByteBufPool alloc) {
      int size = 1 + NAN.length + RespConstants.CRLF.length;
      alloc.acquire(size)
            .writeByte(RespConstants.DOUBLE)
            .writeBytes(NAN)
            .writeBytes(RespConstants.CRLF);
   }

   private void writeInfinite(boolean positive, ByteBufPool alloc) {
      int size = 1 + (positive ? 0 : 1) + INF.length + RespConstants.CRLF.length;
      ByteBuf buffer = alloc.acquire(size).writeByte(RespConstants.DOUBLE);

      if (!positive) buffer.writeByte('-');

      buffer.writeBytes(INF).writeBytes(RespConstants.CRLF);
   }

   private byte[] serializeDouble(double value) {
      return Double.toString(value).getBytes(StandardCharsets.US_ASCII);
   }

   @Override
   public boolean test(Object object) {
      // Any floating point number.
      return object instanceof Double || object instanceof Float;
   }
}
