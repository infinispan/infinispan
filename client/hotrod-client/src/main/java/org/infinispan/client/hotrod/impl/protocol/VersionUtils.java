package org.infinispan.client.hotrod.impl.protocol;

/**
 * Utilities to handle protocol versions
 *
 * @author gustavonalle
 * @since 8.2
 */
public final class VersionUtils {

   public static boolean isVersionGreaterOrEquals(String version, String another) {
      if (!CodecFactory.isVersionDefined(version)) {
         throw new IllegalArgumentException("Invalid Hot Rod protocol version " + version);
      }
      if (!CodecFactory.isVersionDefined(another)) {
         throw new IllegalArgumentException("Invalid Hot Rod protocol version " + another);
      }
      return Float.valueOf(version) >= Float.valueOf(another);
   }

}
