package org.infinispan.server.hotrod.util;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.server.hotrod.HotRodTypeConverter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
public final class BulkUtil {

   // The scope constants correspond to values defined in by the Hot Rod protocol spec
   // (http://infinispan.org/docs/7.0.x/user_guide/user_guide.html#_hot_rod_protocol_1_2)
   public static final int DEFAULT_SCOPE = 0;
   public static final int GLOBAL_SCOPE = 1;
   public static final int LOCAL_SCOPE = 2;

   public static Iterator<byte[]> getAllKeys(Cache<byte[], ?> cache, int scope) {
      CompatibilityModeConfiguration compatibility = cache.getCacheConfiguration().compatibility();
      CacheStream stream = cache.keySet().stream();
      HotRodTypeConverter converter = new HotRodTypeConverter();
      if (compatibility.enabled() && compatibility.marshaller() != null) {
         converter.setMarshaller(compatibility.marshaller());
      }
      return new IteratorMapper<Object, byte[]>(stream.iterator(), k -> {
         if (k instanceof byte[]) {
            return (byte[]) k;
         }
         return (byte[]) converter.unboxKey(k);
      });
   }
}
