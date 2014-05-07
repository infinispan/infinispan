package org.infinispan.filter;

import org.infinispan.metadata.Metadata;

/**
 * Converter that can be used to transform a given entry to a different value.  This is especially useful to reduce
 * overall payload of given data that is sent for the given event when a notification is send to a cluster listener as
 * this will have to be serialized and sent across the network when the cluster listener is not local to the node who
 * owns the given key.
 *
 * @author William Burns
 * @since 7.0
 */
public interface Converter<K, V, C> {
   C convert(K key, V value, Metadata metadata);
}
