package org.infinispan.client.hotrod.event;

/**
 * Client side cache entry modified events provide information on the modified
 * key, and the version of the entry after the modification. This version can
 * be used to invoke conditional operations on the server, such as
 * {@link org.infinispan.client.hotrod.RemoteCache#replaceWithVersion(Object, Object, long)}
 * or {@link org.infinispan.client.hotrod.RemoteCache#removeWithVersion(Object, long)}
 *
 * @param <K> type of key created.
 */
public interface ClientCacheEntryModifiedEvent<K> extends ClientEvent {

   /**
    * Modifiedcache entry's key.
    * @return an instance of the key with which a cache entry has been
    * modified in the remote server(s).
    */
   K getKey();

   /**
    * Provides access to the version of the modified cache entry. This version
    * can be used to invoke conditional operations on the server, such as
    * {@link org.infinispan.client.hotrod.RemoteCache#replaceWithVersion(Object, Object, long)}
    * or {@link org.infinispan.client.hotrod.RemoteCache#removeWithVersion(Object, long)}
    *
    * @return a long containing the version of the modified cache entry.
    */
   long getVersion();

   /**
    * This will be true if the write command that caused this had to be retried
    * again due to a topology change.  This could be a sign that this event
    * has been duplicated or another event was dropped and replaced
    * (eg: ModifiedEvent replaced CreateEvent)
    *
    * @return Whether the command that caused this event was retried
    */
   boolean isCommandRetried();

}
