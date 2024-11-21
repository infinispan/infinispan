package org.infinispan.client.hotrod.event;

/**
 * Client side cache entry removed events provide information on the removed key.
 *
 * @param <K> type of key created.
 */
public interface ClientCacheEntryRemovedEvent<K> extends ClientEvent {

   /**
    * Created cache entry's key.
    * @return an instance of the key with which a cache entry has been
    * created in remote server.
    */
   K getKey();

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
