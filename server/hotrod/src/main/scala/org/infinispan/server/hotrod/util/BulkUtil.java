package org.infinispan.server.hotrod.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distexec.mapreduce.Collator;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 *
 */
public final class BulkUtil {

   // The scope constants correspond to values defined in by the Hot Rod protocol spec
   // (http://infinispan.org/docs/7.0.x/user_guide/user_guide.html#_hot_rod_protocol_1_2)
   public static final int DEFAULT_SCOPE = 0;
	public static final int GLOBAL_SCOPE = 1;
	public static final int LOCAL_SCOPE = 2;

	public static <K> Set<K> getAllKeys(Cache<K, ?> cache, int scope) {
		CacheMode cacheMode = cache.getAdvancedCache().getCacheConfiguration().clustering().cacheMode(); 
		boolean keysAreLocal = !cacheMode.isClustered() || cacheMode.isReplicated();
		if (keysAreLocal || scope == LOCAL_SCOPE) {
			return cache.keySet();
		} else {
         MapReduceTask<K, Object, K, Object> task =
               new MapReduceTask<K, Object, K, Object>((Cache<K, Object>) cache)
               .mappedWith(new KeyMapper<K>())
               .reducedWith(new KeyReducer<K>());
         return task.execute(new KeysCollator<K>());
      }
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
}
