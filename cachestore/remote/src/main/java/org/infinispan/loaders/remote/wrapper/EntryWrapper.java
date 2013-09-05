package org.infinispan.loaders.remote.wrapper;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.persistence.CacheLoaderException;

/**
 * EntryWrapper.
 *
 * @author Tristan Tarrant
 * @since 5.2
 * @deprecated No longer needed since raw types are stored now in Hot Rod
 */
@Deprecated
public interface EntryWrapper<K, V> {

   K wrapKey(Object key) throws CacheLoaderException;

   V wrapValue(MetadataValue<?> value) throws CacheLoaderException;
}
