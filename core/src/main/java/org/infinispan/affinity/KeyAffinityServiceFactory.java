package org.infinispan.affinity;

import org.infinispan.Cache;
import org.infinispan.executors.ExecutorFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class KeyAffinityServiceFactory {

   /**
    * Creates an {@link org.infinispan.affinity.KeyAffinityService} instance.
    *
    * @param cache         the distributed cache for which this service runs
    * @param ex            used for obtaining a thread that async generates keys.
    * @param keyProvider   allows one to control how the generated keys look like.
    * @param keyBufferSize the number of generated keys per {@link org.infinispan.remoting.transport.Address}.
    * @return an {@link org.infinispan.affinity.KeyAffinityService} implementation.
    * @throws IllegalStateException if the supplied cache is not DIST.
    */
   public static <K,V> KeyAffinityService<K> newKeyAffinityService(Cache<K,V> cache, ExecutorFactory ex, KeyProvider keyProvider, int keyBufferSize) {
      return null;
   }

   /**
    * Same as {@link #newKeyAffinityService(org.infinispan.Cache,org.infinispan.executors.ExecutorFactory,
    * KeyProvider,int)} with the an {@link org.infinispan.affinity.RndKeyProvider}.
    */
   public static <K,V> KeyAffinityService newKeyAffinityService(Cache<K,V> cache, ExecutorFactory ex, int keyBufferSize) {
      return newKeyAffinityService(cache, ex, new RndKeyProvider(), keyBufferSize);
   }
   
   /**
    * Same as {@link #newKeyAffinityService(org.infinispan.Cache,org.infinispan.executors.ExecutorFactory,
    * KeyProvider,int)} with the an {@link org.infinispan.affinity.RndKeyProvider} and an
    * {@link java.util.concurrent.Executors#newSingleThreadExecutor()} executor.
    */
   public static <K,V> KeyAffinityService newKeyAffinityService(Cache<K,V> cache, int keyBufferSize) {
      return newKeyAffinityService(cache, new ExecutorFactory() {
         @Override
         public ExecutorService getExecutor(Properties p) {
            return Executors.newSingleThreadExecutor();
         }
      }, new RndKeyProvider(), keyBufferSize);
   }
}
