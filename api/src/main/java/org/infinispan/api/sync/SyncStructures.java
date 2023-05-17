package org.infinispan.api.sync;

/**
 * @since 15.0
 **/
public interface SyncStructures {
   <V> SyncList<V> list(String name);
}
