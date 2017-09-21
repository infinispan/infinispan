package org.infinispan.distexec.spi;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@SuppressWarnings("unused")
public class DefaultDistributedTaskLifecycle implements DistributedTaskLifecycle {

   @Override
   public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputCache, Collection<K> inputKeys) {
      // intentionally no-op
   }

   @Override
   public <T> void onPostExecute(Callable<T> task) {
      // intentionally no-op
   }
}
