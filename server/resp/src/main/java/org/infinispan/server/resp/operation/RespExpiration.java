package org.infinispan.server.resp.operation;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.RespUtil;

public enum RespExpiration {
   EX {
      @Override
      protected long convert(long value, TimeService timeService) {
         return TimeUnit.SECONDS.toMillis(value);
      }
   },
   PX {
      @Override
      protected long convert(long value, TimeService timeService) {
         return value;
      }
   },
   EXAT {
      @Override
      protected long convert(long value, TimeService timeService) {
         return (value - timeService.instant().getEpochSecond()) * 1000;
      }
   },
   PXAT {
      @Override
      protected long convert(long value, TimeService timeService) {
         return value - timeService.instant().toEpochMilli();
      }
   };

   public static final byte[] EX_BYTES = "EX".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] PX_BYTES = "PX".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] EXAT_BYTES = "EXAT".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] PXAT_BYTES = "PXAT".getBytes(StandardCharsets.US_ASCII);

   protected abstract long convert(long value, TimeService timeService);

   public static RespExpiration valueOf(byte[] type) {
      if (type.length == 2) {
         if (!RespUtil.caseInsensitiveAsciiCheck('X', type[1]))
            throw new IllegalArgumentException("Invalid expiration type");

         switch (type[0]) {
            case 'E':
            case 'e':
               return EX;
            case 'P':
            case 'p':
               return PX;
         }
      }

      if (type.length == 4) {
         if (RespUtil.isAsciiBytesEquals(EXAT_BYTES, type)) return EXAT;
         if (RespUtil.isAsciiBytesEquals(PXAT_BYTES, type)) return PXAT;
      }

      throw new IllegalArgumentException("Invalid expiration type");
   }
}
