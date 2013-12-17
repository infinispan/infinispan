package org.infinispan.distexec.mapreduce.spi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class MapReduceTaskLifecycleService {

   private static final Log log = LogFactory.getLog(MapReduceTaskLifecycleService.class);
   private List<MapReduceTaskLifecycle> lifecycles;

   public MapReduceTaskLifecycleService(final ClassLoader cl) {
      ServiceLoader<MapReduceTaskLifecycle> loader = ServiceLoader.load(MapReduceTaskLifecycle.class, cl);
      lifecycles = new ArrayList<MapReduceTaskLifecycle>();
      for (MapReduceTaskLifecycle l : loader) {
         lifecycles.add(l);
      }
   }

   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper,  Cache<KIn, VIn> inputCache) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPreExecute(mapper, inputCache);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPostExecute(mapper);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPreExecute(reducer, inputCache);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
      try {
         for (MapReduceTaskLifecycle l : lifecycles) {
            l.onPostExecute(reducer);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }
}