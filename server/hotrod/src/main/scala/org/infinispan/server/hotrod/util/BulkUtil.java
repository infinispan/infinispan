package org.infinispan.server.hotrod.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Immutables;
import org.infinispan.compat.TypeConverter;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.distexec.mapreduce.Collator;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.server.hotrod.HotRodTypeConverter;

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

   public static Set<byte[]> getAllKeys(Cache<byte[], ?> cache, int scope) {
      CacheMode cacheMode = cache.getAdvancedCache().getCacheConfiguration().clustering().cacheMode();
      boolean keysAreLocal = !cacheMode.isClustered() || cacheMode.isReplicated();
      if (keysAreLocal || scope == LOCAL_SCOPE) {
         return cache.keySet();
      } else {
         MapReduceTask<byte[], Object, byte[], Object> task =
               new MapReduceTask<byte[], Object, byte[], Object>((Cache<byte[], Object>) cache)
                     .mappedWith(new KeyMapper<byte[]>())
                     .reducedWith(new KeyReducer<byte[]>());
         return task.execute(createCollator(cache));
      }
   }

   private static Collator<byte[], Object, Set<byte[]>> createCollator(Cache<byte[], ?> cache) {
      CompatibilityModeConfiguration compatibility = cache.getCacheConfiguration().compatibility();
      boolean enabled = compatibility.enabled();
      return enabled ? new CompatibilityCollator<byte[]>(compatibility.marshaller())
            : new KeysCollator<byte[]>();
   }

   private static class KeyMapper<K> implements Mapper<K, Object, K, Object> {

      private static final long serialVersionUID = -5054573988280497412L;

      @Override
      public void map(K key, Object value, Collector<K, Object> collector) {
         collector.emit(key, null);
      }
   }

   private static class KeyReducer<K> implements Reducer<K, Object> {

      private static final long serialVersionUID = -8199097945001793869L;

      @Override
      public Object reduce(K reducedKey, Iterator<Object> iter) {
         return null;  // the value is not actually used, we can output null
      }
   }

   private static class KeysCollator<K> implements Collator<K, Object, Set<K>> {

      @Override
      public Set<K> collate(Map<K, Object> reducedResults) {
         return reducedResults.keySet();
      }
   }

   private static class CompatibilityCollator<K> implements Collator<K, Object, Set<K>> {

      private final HotRodTypeConverter converter = new HotRodTypeConverter();

      private CompatibilityCollator(Marshaller compatibilityMarshaller) {
         if (compatibilityMarshaller != null)
            converter.setMarshaller(compatibilityMarshaller);
      }

      @Override
      public Set<K> collate(Map<K, Object> reducedResults) {
         Set<K> keySet = reducedResults.keySet();
         Set<K> backingSet = new HashSet<K>(keySet.size());
         for (K key : keySet)
            backingSet.add((K) converter.unboxKey(key));

         return Immutables.immutableSetWrap(backingSet);
      }
   }

}
