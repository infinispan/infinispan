package org.infinispan.scripting.impl;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
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
         applyEnvironment((EnvironmentAware) mapper, inputCache);
      }
   }

   @Override
   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
   }

   @Override
   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
      if(reducer instanceof EnvironmentAware) {
         applyEnvironment((EnvironmentAware) reducer, inputCache);
      }
   }

   @Override
   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
   }

   private void applyEnvironment(EnvironmentAware mapper, Cache<?, ?> cache) {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      CompatibilityModeConfiguration compatibility = cache.getCacheConfiguration().compatibility();
      Marshaller marshaller = compatibility.enabled() ? compatibility.marshaller() : cacheManager.getCacheManagerConfiguration().serialization().marshaller();
      mapper.setEnvironment(cacheManager, marshaller);
   }
}
