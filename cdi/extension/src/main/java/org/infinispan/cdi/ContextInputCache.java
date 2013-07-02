package org.infinispan.cdi;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.mapreduce.Mapper;

/**
 * ContextInputCache keeps track of {@link Input} cache to be injected into Callables from
 * {@link DistributedExecutorService} and {@link Mapper} from {@link MapReduceTask} using CDI
 * mechanism. The cache injected will be the cache used to construct
 * {@link DistributedExecutorService} and/or {@link MapReduceTask}
 * 
 * @author Vladimir Blagoejvic
 * @since 5.2
 * @see InfinispanExtension#registerInputCacheCustomBean(javax.enterprise.inject.spi.AfterBeanDiscovery,
 *      javax.enterprise.inject.spi.BeanManager)
 * 
 */
public class ContextInputCache {

   /*
    * Using thread local was the last choice. See https://issues.jboss.org/browse/ISPN-2181 for more
    * details and design decisions made
    */
   private static ThreadLocal<Cache<?, ?>> contextualCache = new ThreadLocal<Cache<?, ?>>();

   public static <K, V> void set(Cache<K, V> input) {
      contextualCache.set(input);
   }

   @SuppressWarnings("unchecked")
   public static <K, V> Cache<K, V> get() {
      return (Cache<K, V>) contextualCache.get();
   }

   public static void clean() {
      contextualCache.remove();
   }

}
