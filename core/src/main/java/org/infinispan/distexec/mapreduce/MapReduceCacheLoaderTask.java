package org.infinispan.distexec.mapreduce;

import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledValue;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class MapReduceCacheLoaderTask implements AdvancedCacheLoader.CacheLoaderTask {

   final Mapper mapper;
   final Collector collector;

   public MapReduceCacheLoaderTask(Mapper mapper, Collector collector) {
      this.mapper = mapper;
      this.collector = collector;
   }

   @Override
   public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) {
      mapper.map(marshalledEntry.getKey(), getValue(marshalledEntry), collector);
   }

   private Object getValue(MarshalledEntry marshalledEntry) {
      Object loadedValue = marshalledEntry.getValue();
      if (loadedValue instanceof MarshalledValue) {
         return  ((MarshalledValue) loadedValue).get();
      } else {
         return loadedValue;
      }
   }
}
