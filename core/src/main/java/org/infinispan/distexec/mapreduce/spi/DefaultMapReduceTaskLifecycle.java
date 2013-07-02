package org.infinispan.distexec.mapreduce.spi;

import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;

public class DefaultMapReduceTaskLifecycle implements MapReduceTaskLifecycle {

   @Override
   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper,  Cache<KIn, VIn> inputCache) {
      // intentionally no-op
   }

   @Override
   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      // intentionally no-op
   }

   @Override
   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
   // intentionally no-op
   }

   @Override
   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
   // intentionally no-op
   }

}
