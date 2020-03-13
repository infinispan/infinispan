package org.infinispan.commons.util;

public enum MemoryUnit {
   BYTES("B") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return size;
      }

      @Override
      public long toKiloBytes(long size) {
         return size / KILO;
      }

      @Override
      public long toKibiBytes(long size) {
         return size / KIBI;
      }

      @Override
      public long toMegaBytes(long size) {
         return size / MEGA;
      }

      @Override
      public long toMebiBytes(long size) {
         return size / MEBI;
      }

      @Override
      public long toGigaBytes(long size) {
         return size / GIGA;
      }

      @Override
      public long toGibiBytes(long size) {
         return size / GIBI;
      }

      @Override
      public long toTeraBytes(long size) {
         return size / TERA;
      }

      @Override
      public long toTebiBytes(long size) {
         return size / TEBI;
      }
   },

   KILOBYTES("K") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toKiloBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, KILO, MAX / KILO);
      }

      @Override
      public long toKiloBytes(long size) {
         return size;
      }

      @Override
      public long toKibiBytes(long size) {
         return f(size, KILO, KIBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return size / KILO;
      }

      @Override
      public long toMebiBytes(long size) {
         return f(size, KILO, MEBI);
      }

      @Override
      public long toGigaBytes(long size) {
         return size / MEGA;
      }

      @Override
      public long toGibiBytes(long size) {
         return f(size, KILO, GIBI);
      }

      @Override
      public long toTeraBytes(long size) {
         return size / GIGA;
      }

      @Override
      public long toTebiBytes(long size) {
         return f(size, KILO, TEBI);
      }

   },

   KIBIBYTES("Ki") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toKibiBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, KIBI, MAX / KIBI);
      }

      @Override
      public long toKiloBytes(long size) {
         return f(size, KIBI, KILO);
      }

      @Override
      public long toKibiBytes(long size) {
         return size;
      }

      @Override
      public long toMegaBytes(long size) {
         return f(size, KIBI, MEGA);
      }

      @Override
      public long toMebiBytes(long size) {
         return size / KIBI;
      }

      @Override
      public long toGigaBytes(long size) {
         return f(size, KIBI, GIGA);
      }

      @Override
      public long toGibiBytes(long size) {
         return size / MEBI;
      }

      @Override
      public long toTeraBytes(long size) {
         return f(size, KIBI, TERA);
      }

      @Override
      public long toTebiBytes(long size) {
         return size / GIBI;
      }

   },

   MEGABYTES("M") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toMegaBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, MEGA, MAX / MEGA);
      }

      @Override
      public long toKiloBytes(long size) {
         return x(size, KILO, MAX / KILO);
      }

      @Override
      public long toKibiBytes(long size) {
         return f(size, MEGA, KIBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return size;
      }

      @Override
      public long toMebiBytes(long size) {
         return f(size, MEGA, MEBI);
      }

      @Override
      public long toGigaBytes(long size) {
         return size / KILO;
      }

      @Override
      public long toGibiBytes(long size) {
         return f(size, MEGA, GIBI);
      }

      @Override
      public long toTeraBytes(long size) {
         return size / MEGA;
      }

      @Override
      public long toTebiBytes(long size) {
         return f(size, MEGA, TEBI);
      }

   },

   MEBIBYTES("Mi") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toMebiBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, MEBI, MAX / MEBI);
      }

      @Override
      public long toKiloBytes(long size) {
         return f(size, MEBI, KILO);
      }

      @Override
      public long toKibiBytes(long size) {
         return x(size, KIBI, MAX / KIBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return f(size, MEBI, MEGA);
      }

      @Override
      public long toMebiBytes(long size) {
         return size;
      }

      @Override
      public long toGigaBytes(long size) {
         return f(size, MEBI, GIGA);
      }

      @Override
      public long toGibiBytes(long size) {
         return size / KIBI;
      }

      @Override
      public long toTeraBytes(long size) {
         return f(size, MEBI, TERA);
      }

      @Override
      public long toTebiBytes(long size) {
         return size / MEBI;
      }

   },

   GIGABYTES("G") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toGigaBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, GIGA, MAX / GIGA);
      }

      @Override
      public long toKiloBytes(long size) {
         return x(size, MEGA, MAX / MEGA);
      }

      @Override
      public long toKibiBytes(long size) {
         return f(size, GIGA, KIBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return x(size, KILO, MAX / KILO);
      }

      @Override
      public long toMebiBytes(long size) {
         return f(size, GIGA, MEBI);
      }

      @Override
      public long toGigaBytes(long size) {
         return size;
      }

      @Override
      public long toGibiBytes(long size) {
         return f(size, GIGA, GIBI);
      }

      @Override
      public long toTeraBytes(long size) {
         return size / KILO;
      }

      @Override
      public long toTebiBytes(long size) {
         return f(size, GIGA, TEBI);
      }

   },

   GIBIBYTES("Gi") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toGibiBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, GIBI, MAX / GIBI);
      }

      @Override
      public long toKiloBytes(long size) {
         return f(size, GIBI, KILO);
      }

      @Override
      public long toKibiBytes(long size) {
         return x(size, MEBI, MAX / MEBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return f(size, GIBI, MEGA);
      }

      @Override
      public long toMebiBytes(long size) {
         return x(size, KIBI, MAX / KIBI);
      }

      @Override
      public long toGigaBytes(long size) {
         return f(size, GIBI, GIGA);
      }

      @Override
      public long toGibiBytes(long size) {
         return size;
      }

      @Override
      public long toTeraBytes(long size) {
         return f(size, GIBI, TERA);
      }

      @Override
      public long toTebiBytes(long size) {
         return size / KIBI;
      }

   },

   TERABYTES("T") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toTeraBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, TERA, MAX / TERA);
      }

      @Override
      public long toKiloBytes(long size) {
         return x(size, GIGA, MAX / GIGA);
      }

      @Override
      public long toKibiBytes(long size) {
         return f(size, TERA, KIBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return x(size, MEGA, MAX / MEGA);
      }

      @Override
      public long toMebiBytes(long size) {
         return f(size, TERA, MEBI);
      }

      @Override
      public long toGigaBytes(long size) {
         return x(size, KILO, MAX / KILO);
      }

      @Override
      public long toGibiBytes(long size) {
         return f(size, TERA, GIBI);
      }

      @Override
      public long toTeraBytes(long size) {
         return size;
      }

      @Override
      public long toTebiBytes(long size) {
         return f(size, TERA, TEBI);
      }

   },

   TEBIBYTES("Ti") {

      @Override
      public long convert(long sourceSize, MemoryUnit sourceUnit) {
         return sourceUnit.toTebiBytes(sourceSize);
      }

      @Override
      public long toBytes(long size) {
         return x(size, TEBI, MAX / TEBI);
      }

      @Override
      public long toKiloBytes(long size) {
         return f(size, TEBI, KILO);
      }

      @Override
      public long toKibiBytes(long size) {
         return x(size, GIBI, MAX / GIBI);
      }

      @Override
      public long toMegaBytes(long size) {
         return f(size, TEBI, MEGA);
      }

      @Override
      public long toMebiBytes(long size) {
         return x(size, MEBI, MAX / MEBI);
      }

      @Override
      public long toGigaBytes(long size) {
         return f(size, TEBI, GIGA);
      }

      @Override
      public long toGibiBytes(long size) {
         return x(size, KIBI, MAX / KIBI);
      }

      @Override
      public long toTeraBytes(long size) {
         return f(size, TEBI, TERA);
      }

      @Override
      public long toTebiBytes(long size) {
         return size;
      }
   };

   private static final long KILO = 1000;
   private static final long KIBI = 1024;
   private static final long MEGA = KILO * KILO;
   private static final long MEBI = KIBI * KIBI;
   private static final long GIGA = KILO * MEGA;
   private static final long GIBI = KIBI * MEBI;
   private static final long TERA = KILO * GIGA;
   private static final long TEBI = KIBI * GIBI;
   static final long MAX = Long.MAX_VALUE;

   private final String suffix;

   MemoryUnit(String suffix) {
      this.suffix = suffix;
   }

   public String getSuffix() {
      return suffix;
   }

   static long f(long d, long numerator, long denominator) {
      return (long) (((float)d) * ((float)numerator) / (denominator));
   }
;
   static long x(long d, long m, long over) {
      if (d > over)
         return Long.MAX_VALUE;
      if (d < -over)
         return Long.MIN_VALUE;
      return d * m;
   }

   public long convert(long sourceSize, MemoryUnit sourceUnit) {
      throw new AbstractMethodError();
   }

   public long toBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toKiloBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toKibiBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toMegaBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toMebiBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toGigaBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toGibiBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toTeraBytes(long size) {
      throw new AbstractMethodError();
   }

   public long toTebiBytes(long size) {
      throw new AbstractMethodError();
   }

   public static long parseBytes(String s) {
      if (s == null)
         throw new NullPointerException();
      int us = s.length();
      while (us > 0 && !Character.isDigit(s.charAt(us - 1))) {
         us--;
      }
      if (us == s.length()) {
         return Long.parseLong(s);
      }
      String suffix = s.substring(us);
      for(MemoryUnit u : MemoryUnit.values()) {
         if (u.suffix.equals(suffix)) {
            long size = Long.parseLong(s.substring(0, us));
            return u.toBytes(size);
         }
      }
      throw new IllegalArgumentException(s);
   }

}
