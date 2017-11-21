package org.infinispan.client.hotrod.multimap;

import java.util.concurrent.CompletableFuture;

import org.infinispan.multimap.api.BasicMultimapCache;

/**
 * {@inheritDoc}
 * <p>
 * Remote MultimapCache interface used for server mode.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
public interface RemoteMultimapCache<K, V> extends BasicMultimapCache<K, V> {

   /**
    * Returns a {@link MetadataCollection<V>} of the values associated with key in this multimap cache,
    * if any. Any changes to the retrieved collection won't change the values in this multimap cache.
    * <b>When this method returns an empty metadata collection, it means the key was not found.</b>
    *
    * @param key to be retrieved
    * @return the collection with the metadata of the given key
    */
   CompletableFuture<MetadataCollection<V>> getWithMetadata(K key);

}
