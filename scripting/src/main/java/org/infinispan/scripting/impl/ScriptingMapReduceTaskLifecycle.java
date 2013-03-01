package org.infinispan.scripting.impl;

import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycle;
import org.kohsuke.MetaInfServices;

/**
 * ScriptingMapReduceTaskLifecycle.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@MetaInfServices
public class ScriptingMapReduceTaskLifecycle implements MapReduceTaskLifecycle {

   @Override
   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache) {
      if(mapper instanceof EnvironmentAware) {
         ((EnvironmentAware)mapper).setEnvironment(inputCache.getCacheManager());
      }
   }

   @Override
   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
   }

   @Override
   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
      if(reducer instanceof EnvironmentAware) {
         ((EnvironmentAware)reducer).setEnvironment(inputCache.getCacheManager());
      }
   }

   @Override
   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
   }


}
