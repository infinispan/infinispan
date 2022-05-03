package org.infinispan.hotrod.impl.cache;

/**
 * Besides the key and value, also contains an version. To be used in versioned operations, e.g. {@link
 * RemoteCache#removeWithVersion(Object, long)}.
 *
 */
public interface VersionedValue<V> extends Versioned {

   V getValue();
}
