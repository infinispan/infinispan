package org.infinispan.distexec.spi;

import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class DefaultDistributedTaskLifecycle implements DistributedTaskLifecycle {

   @Override
   public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputCache) {
      // intentionally no-op
   }

   @Override
   public <T> void onPostExecute(Callable<T> task) {
      // intentionally no-op
   }
}
