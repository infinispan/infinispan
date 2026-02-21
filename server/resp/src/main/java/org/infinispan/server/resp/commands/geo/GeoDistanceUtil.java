package org.infinispan.server.resp.commands.geo;

/**
 * Distance calculation utilities for GEO commands.
 * Uses the Haversine formula to calculate great-circle distances between points on Earth.
 *
 * @since 16.2
 */
public final class GeoDistanceUtil {

   /**
    * Earth's radius in meters (same value Redis uses).
    */
   public static final double EARTH_RADIUS_METERS = 6372797.560856;

   private GeoDistanceUtil() {
   }

   /**
    * Calculate the distance in meters between two points using the Haversine formula.
    *
    * @param lon1 longitude of first point in degrees
    * @param lat1 latitude of first point in degrees
    * @param lon2 longitude of second point in degrees
    * @param lat2 latitude of second point in degrees
    * @return distance in meters
    */
   public static double haversine(double lon1, double lat1, double lon2, double lat2) {
      double lat1Rad = Math.toRadians(lat1);
      double lat2Rad = Math.toRadians(lat2);
      double dLatRad = Math.toRadians(lat2 - lat1);
      double dLonRad = Math.toRadians(lon2 - lon1);

      double a = Math.sin(dLatRad / 2) * Math.sin(dLatRad / 2) +
            Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                  Math.sin(dLonRad / 2) * Math.sin(dLonRad / 2);
      double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

      return EARTH_RADIUS_METERS * c;
   }

   /**
    * Convert distance from meters to the specified unit.
    *
    * @param meters distance in meters
    * @param unit   target unit
    * @return distance in the target unit
    */
   public static double convertTo(double meters, GeoUnit unit) {
      return unit.fromMeters(meters);
   }

   /**
    * Check if a point is within a bounding box centered at a point.
    * The box is defined by width (longitude span) and height (latitude span).
    *
    * @param centerLon longitude of center point
    * @param centerLat latitude of center point
    * @param pointLon  longitude of point to check
    * @param pointLat  latitude of point to check
    * @param width     width of the box (longitude span)
    * @param height    height of the box (latitude span)
    * @param unit      unit of width and height
    * @return true if the point is within the box
    */
   public static boolean withinBox(double centerLon, double centerLat,
                                   double pointLon, double pointLat,
                                   double width, double height, GeoUnit unit) {
      // Convert box dimensions to meters
      double widthMeters = unit.toMeters(width);
      double heightMeters = unit.toMeters(height);

      // Calculate distances in longitude and latitude directions
      // For longitude distance, we need to account for latitude
      double dLon = haversine(centerLon, centerLat, pointLon, centerLat);
      double dLat = haversine(centerLon, centerLat, centerLon, pointLat);

      // Check if within half-width and half-height
      return dLon <= widthMeters / 2 && dLat <= heightMeters / 2;
   }
}
