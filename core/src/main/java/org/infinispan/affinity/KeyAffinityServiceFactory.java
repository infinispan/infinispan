package org.infinispan.affinity;

import org.infinispan.Cache;
import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;

/**
 * Factory for {@link org.infinispan.affinity.KeyAffinityService}.
 * Services build by this factory have the following characteristics:
 * <ul>
 *  <li>are run asynchronously by a thread that can be plugged through an {@link org.infinispan.commons.executors.ExecutorFactory} </li>
 *  <li>for key generation, the {@link org.infinispan.distribution.ch.ConsistentHash} function of a distributed cache is used. Service does not make sense for replicated caches.</li>
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
    * Creates an {@link org.infinispan.affinity.KeyAffinityService} instance that generates keys mapped to all addresses
    * in the cluster. Changes in topology would also noticed: by adding a new node, the service will automatically start
    * generating keys for it.
    *
    * @param cache         the distributed cache for which this service runs
    * @param ex            used for running async key generation process. On service shutdown, the executor won't be
    *                      stopped; i.e. it's user responsibility manage it's lifecycle.
    * @param keyGenerator  allows one to control how the generated keys look like.
    * @param keyBufferSize the number of generated keys per {@link org.infinispan.remoting.transport.Address}.
    * @param start         weather to start the service or not
    * @return an {@link org.infinispan.affinity.KeyAffinityService} implementation.
    * @throws IllegalStateException if the supplied cache is not DIST.
    */
   public static <K, V> KeyAffinityService<K> newKeyAffinityService(Cache<K, V> cache, Executor ex, KeyGenerator<K> keyGenerator, int keyBufferSize, boolean start) {
      return new KeyAffinityServiceImpl<K>(ex, cache, keyGenerator, keyBufferSize, null, start);
   }

   /**
    * Same as {@link #newKeyAffinityService(org.infinispan.Cache, java.util.concurrent.Executor, KeyGenerator, int,
    * boolean)} with start == true;
    */
   public static <K, V> KeyAffinityService<K> newKeyAffinityService(Cache<K, V> cache, Executor ex, KeyGenerator<K> keyGenerator, int keyBufferSize) {
      return newKeyAffinityService(cache, ex, keyGenerator, keyBufferSize, true);
   }

   /**
    * Creates a service that would only generate keys for addresses specified in filter.
    *
    * @param filter the set of addresses for which to generate keys
    */
   public static <K, V> KeyAffinityService<K> newKeyAffinityService(Cache<K, V> cache, Collection<Address> filter, KeyGenerator<K> keyGenerator, Executor ex, int keyBufferSize, boolean start) {
      return new KeyAffinityServiceImpl<K>(ex, cache, keyGenerator, keyBufferSize, filter, start);
   }

   /**
    * Same as {@link #newKeyAffinityService(org.infinispan.Cache, java.util.Collection, KeyGenerator,
    * java.util.concurrent.Executor, int, boolean)} with start == true.
    */
   public static <K, V> KeyAffinityService<K> newKeyAffinityService(Cache<K, V> cache, Collection<Address> filter, KeyGenerator<K> keyGenerator, Executor ex, int keyBufferSize) {
      return newKeyAffinityService(cache, filter, keyGenerator, ex, keyBufferSize, true);
   }

   /**
    * Created an service that only generates keys for the local address.
    */
   public static <K, V> KeyAffinityService<K> newLocalKeyAffinityService(Cache<K, V> cache, KeyGenerator<K> keyGenerator, Executor ex, int keyBufferSize, boolean start) {
      Address localAddress = cache.getAdvancedCache().getRpcManager().getTransport().getAddress();
      Collection<Address> forAddresses = Collections.singletonList(localAddress);
      return newKeyAffinityService(cache, forAddresses, keyGenerator, ex, keyBufferSize, start);
   }

   /**
    * Same as {@link #newLocalKeyAffinityService(org.infinispan.Cache, KeyGenerator, java.util.concurrent.Executor, int, boolean)} with start == true.
    */
   public static <K, V> KeyAffinityService<K> newLocalKeyAffinityService(Cache<K, V> cache, KeyGenerator<K> keyGenerator, Executor ex, int keyBufferSize) {
      return newLocalKeyAffinityService(cache, keyGenerator, ex, keyBufferSize, true);
   }
}
