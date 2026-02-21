package org.infinispan.server.resp.commands.geo;

/**
 * Distance units for GEO commands.
 *
 * @since 16.2
 */
public enum GeoUnit {
   M(1.0),
   KM(1000.0),
   MI(1609.344),
   FT(0.3048);

   private final double meters;

   GeoUnit(double meters) {
      this.meters = meters;
   }

   public double toMeters(double value) {
      return value * meters;
   }

   public double fromMeters(double value) {
      return value / meters;
   }

   public static GeoUnit parse(byte[] arg) {
      if (arg == null || arg.length == 0) {
         return null;
      }
      String s = new String(arg).toUpperCase();
      return switch (s) {
         case "M" -> M;
         case "KM" -> KM;
         case "MI" -> MI;
         case "FT" -> FT;
         default -> null;
      };
   }
}
