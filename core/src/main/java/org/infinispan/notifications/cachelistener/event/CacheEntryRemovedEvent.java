package org.infinispan.notifications.cachelistener.event;

import org.infinispan.metadata.Metadata;

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}.
 * The {@link #getValue()} method would return the <i>old</i> value prior to deletion, if <code>isPre()</code> is <code>true</code>.
 * If <code>isPre()</code> is <code>false</code>, {@link #getValue()} will return <code>null</code>.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryRemovedEvent<K, V> extends CacheEntryEvent<K, V> {

   /**
    * Retrieves the value of the entry being deleted.
       * @return the value of the entry being deleted, if <code>isPre()</code> is <code>true</code>.  <code>null</code> otherwise.
    */
   V getValue();

   /**
    * Regardless of whether <code>isPre()</code> is <code>true</code> or is
    * <code>false</code>, this method returns the value of the entry being
    * deleted. This method is useful for situations where cache listeners
    * need to know what the old value being deleted is when getting
    * <code>isPre()</code> is <code>false</code> callbacks.
    *
    * @return the value of the entry being deleted, regardless of
    * <code>isPre()</code> value
    */
   V getOldValue();

   /**
    * Regardless of whether <code>isPre()</code> is <code>true</code> or is
    * <code>false</code>, this method returns the metadata of the entry being
    * deleted. This method is useful for situations where cache listeners
    * need to know what the old value being deleted is when getting
    * <code>isPre()</code> is <code>false</code> callbacks.
    *
    * @return the metadata of the entry being deleted, regardless of
    * <code>isPre()</code> value
    */
   Metadata getOldMetadata();

   /**
    * This will be true if the write command that caused this had to be retried again due to a topology change.  This
    * could be a sign that this event has been duplicated or another event was dropped and replaced
    * (eg: ModifiedEvent replaced CreateEvent)
    * @return Whether the command that caused this event was retried
    */
   boolean isCommandRetried();
}
