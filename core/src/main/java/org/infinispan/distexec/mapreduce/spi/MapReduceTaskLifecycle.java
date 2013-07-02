package org.infinispan.distexec.mapreduce.spi;

import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;

public interface MapReduceTaskLifecycle {

   <KIn, VIn, KOut, VOut> void onPreExecute(Mapper <KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache);

   <KIn, VIn, KOut, VOut> void onPostExecute(Mapper <KIn, VIn, KOut, VOut> mapper);
   
   <KOut, VOut> void onPreExecute(Reducer <KOut, VOut> reducer, Cache<?, ?> inputCache);

   <KOut, VOut> void onPostExecute(Reducer <KOut, VOut> reducer);
}
