package org.infinispan.affinity;

import org.infinispan.Cache;
import org.infinispan.executors.ExecutorFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Factory for {@link org.infinispan.affinity.KeyAffinityService}.
 * Services build by this factory have the following characteristics:
 * <ul>
 *  <li>are run asynchronously by a thread that can be plugged through an {@link org.infinispan.executors.ExecutorFactory} </li>
 *  <li>for key generation, the {@link org.infinispan.distribution.ConsistentHash} function of a distributed cache is used. Service does not make sense for replicated caches.</li>
 *  <li>for each address cluster member (identified by an {@link org.infinispan.remoting.transport.Address} member, a fixed number of keys is generated</li>
 * </ul>
 *
 * @see org.infinispan.affinity.KeyAffinityService
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
    * @param keyGenerator   allows one to control how the generated keys look like.
    * @param keyBufferSize the number of generated keys per {@link org.infinispan.remoting.transport.Address}.
    * @return an {@link org.infinispan.affinity.KeyAffinityService} implementation.
    * @throws IllegalStateException if the supplied cache is not DIST.
    */
   public static <K,V> KeyAffinityService<K> newKeyAffinityService(Cache<K,V> cache, ExecutorFactory ex, KeyGenerator keyGenerator, int keyBufferSize) {
      return null;
   }

   /**
    * Same as {@link #newKeyAffinityService(org.infinispan.Cache,org.infinispan.executors.ExecutorFactory,
    * KeyGenerator ,int)} with the an {@link RndKeyGenerator}.
    */
   public static <K,V> KeyAffinityService newKeyAffinityService(Cache<K,V> cache, ExecutorFactory ex, int keyBufferSize) {
      return newKeyAffinityService(cache, ex, new RndKeyGenerator(), keyBufferSize);
   }
   
   /**
    * Same as {@link #newKeyAffinityService(org.infinispan.Cache,org.infinispan.executors.ExecutorFactory,
    * KeyGenerator ,int)} with the an {@link RndKeyGenerator} and an
    * {@link java.util.concurrent.Executors#newSingleThreadExecutor()} executor.
    */
   public static <K,V> KeyAffinityService newKeyAffinityService(Cache<K,V> cache, int keyBufferSize) {
      return newKeyAffinityService(cache, new ExecutorFactory() {
         @Override
         public ExecutorService getExecutor(Properties p) {
            return Executors.newSingleThreadExecutor();
         }
      }, new RndKeyGenerator(), keyBufferSize);
   }
}
