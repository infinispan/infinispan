package org.infinispan.distexec.mapreduce;

import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledValue;

/**
 * This is an internal class, not intended to be used by clients.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class MapReduceCacheLoaderTask<KIn, VIn, KOut, VOut> implements AdvancedCacheLoader.CacheLoaderTask<KIn, VIn> {

   final Mapper<KIn, VIn, KOut, VOut> mapper;
   final Collector<KOut, VOut> collector;

   public MapReduceCacheLoaderTask(Mapper<KIn, VIn, KOut, VOut> mapper, Collector<KOut, VOut> collector) {
      this.mapper = mapper;
      this.collector = collector;
   }

   @Override
   public void processEntry(MarshalledEntry<KIn, VIn> marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) {
      mapper.map(marshalledEntry.getKey(), getValue(marshalledEntry), collector);
   }

   @SuppressWarnings("unchecked")
   private VIn getValue(MarshalledEntry<KIn, VIn> marshalledEntry) {
      Object loadedValue = marshalledEntry.getValue();
      if (loadedValue instanceof MarshalledValue) {
         return  (VIn)((MarshalledValue) loadedValue).get();
      } else {
         return (VIn) loadedValue;
      }
   }
}
