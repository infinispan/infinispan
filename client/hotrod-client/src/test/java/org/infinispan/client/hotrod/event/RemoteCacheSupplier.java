package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;

public interface RemoteCacheSupplier<K> {
   <V> RemoteCache<K, V> get();
}
