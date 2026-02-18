package org.infinispan.server.resp.commands.geo;

/**
 * Geohash encoding/decoding utilities for Redis GEO commands.
 * <p>
 * Redis uses a 52-bit geohash (26 bits for longitude, 26 bits for latitude) stored
 * as a sorted set score (double). This class provides the encoding/decoding
 * algorithms to convert between (longitude, latitude) coordinates and geohash values.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Geohash">Geohash</a>
 * @since 16.2
 */
public final class GeoHashUtil {

   /**
    * Number of bits used for each coordinate (longitude and latitude).
    * Redis uses 26 bits each for a total of 52 bits.
    */
   public static final int GEO_STEP = 26;

   /**
    * Minimum valid longitude.
    */
   public static final double LON_MIN = -180.0;

   /**
    * Maximum valid longitude.
    */
   public static final double LON_MAX = 180.0;

   /**
    * Minimum valid latitude (Web Mercator limit).
    */
   public static final double LAT_MIN = -85.05112878;

   /**
    * Maximum valid latitude (Web Mercator limit).
    */
   public static final double LAT_MAX = 85.05112878;

   /**
    * Base32 alphabet used for geohash string encoding.
    * This is the standard geohash alphabet (not standard base32).
    */
   private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

   private GeoHashUtil() {
   }

   /**
    * Encode longitude and latitude to a 52-bit geohash.
    *
    * @param longitude longitude in degrees (-180 to 180)
    * @param latitude  latitude in degrees (-85.05112878 to 85.05112878)
    * @return 52-bit geohash value
    */
   public static long encode(double longitude, double latitude) {
      long lonBits = encodeRange(longitude, LON_MIN, LON_MAX);
      long latBits = encodeRange(latitude, LAT_MIN, LAT_MAX);
      return interleave(lonBits, latBits);
   }

   /**
    * Decode a 52-bit geohash to longitude and latitude.
    *
    * @param geohash 52-bit geohash value
    * @return array of [longitude, latitude]
    */
   public static double[] decode(long geohash) {
      long[] bits = deinterleave(geohash);
      double lon = decodeRange(bits[0], LON_MIN, LON_MAX);
      double lat = decodeRange(bits[1], LAT_MIN, LAT_MAX);
      return new double[]{lon, lat};
   }

   /**
    * Convert a sorted set score (double) to a geohash (long).
    *
    * @param score the sorted set score
    * @return the geohash value
    */
   public static long scoreToGeohash(double score) {
      return (long) score;
   }

   /**
    * Convert a geohash (long) to a sorted set score (double).
    *
    * @param geohash the geohash value
    * @return the sorted set score
    */
   public static double geohashToScore(long geohash) {
      return (double) geohash;
   }

   /**
    * Convert a 52-bit geohash to an 11-character base32 string.
    * This is used by the GEOHASH command.
    *
    * @param geohash 52-bit geohash value
    * @return 11-character base32 string
    */
   public static String toBase32(long geohash) {
      StringBuilder sb = new StringBuilder(11);
      // Process 5 bits at a time, from most significant to least significant
      // 52 bits = 10 full groups of 5 + 2 remaining bits
      // We need 11 characters, so we process as if we have 55 bits (padding with 0s)
      long hash = geohash << 3; // Shift left by 3 to align with 55 bits (11 * 5)
      for (int i = 0; i < 11; i++) {
         int idx = (int) ((hash >> (55 - 5 - i * 5)) & 0x1F);
         sb.append(BASE32.charAt(idx));
      }
      return sb.toString();
   }

   /**
    * Check if the given longitude is valid.
    *
    * @param longitude longitude to check
    * @return true if valid
    */
   public static boolean isValidLongitude(double longitude) {
      return longitude >= LON_MIN && longitude <= LON_MAX;
   }

   /**
    * Check if the given latitude is valid.
    *
    * @param latitude latitude to check
    * @return true if valid
    */
   public static boolean isValidLatitude(double latitude) {
      return latitude >= LAT_MIN && latitude <= LAT_MAX;
   }

   /**
    * Encode a value within a range to a specified number of bits using binary search.
    */
   private static long encodeRange(double value, double min, double max) {
      if (value < min) value = min;
      if (value > max) value = max;

      // Normalize to [0, 1]
      double normalized = (value - min) / (max - min);
      // Scale to the bit range
      long maxVal = 1L << GEO_STEP;
      long result = (long) (normalized * maxVal);
      // Clamp to valid range
      if (result >= maxVal) {
         result = maxVal - 1;
      }
      return result;
   }

   /**
    * Decode a bit value back to a value within the given range.
    */
   private static double decodeRange(long bits, double min, double max) {
      long maxVal = 1L << GEO_STEP;
      // Calculate the center of the cell
      double normalized = (bits + 0.5) / maxVal;
      return min + normalized * (max - min);
   }

   /**
    * Interleave longitude and latitude bits to create the geohash.
    * Longitude bits go in even positions, latitude bits go in odd positions.
    */
   private static long interleave(long lon, long lat) {
      long result = 0;
      for (int i = 0; i < GEO_STEP; i++) {
         // Extract bit i from each coordinate
         long lonBit = (lon >> (GEO_STEP - 1 - i)) & 1;
         long latBit = (lat >> (GEO_STEP - 1 - i)) & 1;
         // Place longitude bit at position 2*i from MSB (even position)
         // Place latitude bit at position 2*i+1 from MSB (odd position)
         result |= (lonBit << (52 - 1 - 2 * i));
         result |= (latBit << (52 - 2 - 2 * i));
      }
      return result;
   }

   /**
    * Deinterleave a geohash to extract longitude and latitude bits.
    *
    * @param geohash the 52-bit geohash
    * @return array of [longitude bits, latitude bits]
    */
   private static long[] deinterleave(long geohash) {
      long lon = 0;
      long lat = 0;
      for (int i = 0; i < GEO_STEP; i++) {
         // Extract longitude bit from even position (2*i from MSB)
         long lonBit = (geohash >> (52 - 1 - 2 * i)) & 1;
         // Extract latitude bit from odd position (2*i+1 from MSB)
         long latBit = (geohash >> (52 - 2 - 2 * i)) & 1;
         // Place in result
         lon |= (lonBit << (GEO_STEP - 1 - i));
         lat |= (latBit << (GEO_STEP - 1 - i));
      }
      return new long[]{lon, lat};
   }
}
