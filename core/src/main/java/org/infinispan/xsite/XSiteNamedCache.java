package org.infinispan.xsite;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.util.ByteString;

/**
 * Caches the site name {@link String} and {@link ByteString} instances.
 *
 * @since 14.0
 */
public final class XSiteNamedCache {

   private static final Map<String, ByteString> CACHE = new ConcurrentHashMap<>(4);

   private XSiteNamedCache() {
   }

   public static ByteString cachedByteString(String siteName) {
      return CACHE.computeIfAbsent(siteName, ByteString::fromString);
   }

   public static String cachedString(String siteName) {
      return cachedByteString(siteName).toString();
   }

   public static ByteString cachedByteString(ByteString siteName) {
      ByteString old = CACHE.putIfAbsent(siteName.toString(), siteName);
      return old == null ? siteName : old;
   }
}
