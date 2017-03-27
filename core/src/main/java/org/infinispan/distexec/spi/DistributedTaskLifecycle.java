package org.infinispan.distexec.spi;

import java.util.Collection;
import java.util.concurrent.Callable;

import org.infinispan.Cache;

public interface DistributedTaskLifecycle {

   <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache, Collection<K> inputKeys);

   <T> void onPostExecute(Callable<T> task);
}
