package org.infinispan.distexec.spi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class DistributedTaskLifecycleService {
   private static final Log log = LogFactory.getLog(DistributedTaskLifecycleService.class);
   private static DistributedTaskLifecycleService service;
   private final List<DistributedTaskLifecycle> lifecycles;

   private DistributedTaskLifecycleService() {
      lifecycles = new ArrayList<DistributedTaskLifecycle>();
      for (DistributedTaskLifecycle cl : ServiceFinder.load(DistributedTaskLifecycle.class)) {
         lifecycles.add(cl);
      }
   }

   public static synchronized DistributedTaskLifecycleService getInstance() {
      if (service == null) {
         service = new DistributedTaskLifecycleService();
      }
      return service;
   }

   public <T,K,V> void onPreExecute(Callable<T> task, Cache <K,V> inputCache) {
      try {
         for (DistributedTaskLifecycle l : lifecycles) {
            l.onPreExecute(task, inputCache);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }

   public <T> void onPostExecute(Callable<T> task) {
      try {
         for (DistributedTaskLifecycle l : lifecycles) {
            l.onPostExecute(task);
         }
      } catch (ServiceConfigurationError serviceError) {
         log.errorReadingProperties(new IOException(
                  "Could not properly load and instantiate DistributedTaskLifecycle service ",
                  serviceError));
      }
   }
}