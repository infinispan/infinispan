package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}.
 * <p />
 * The {@link #getValue()} method would return the <i>old</i> value prior to deletion, if <tt>isPre()</tt> is <tt>true</tt>.
 * If <tt>isPre()</tt> is <tt>false</tt>, {@link #getValue()} will return <tt>null</tt>.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryRemovedEvent<K, V> extends CacheEntryEvent<K, V> {

   /**
    * Retrieves the value of the entry being deleted.
    * <p />
    * @return the value of the entry being deleted, if <tt>isPre()</tt> is <tt>true</tt>.  <tt>null</tt> otherwise.
    */
   V getValue();

   /**
    * Regardless of whether <tt>isPre()</tt> is <tt>true</tt> or is
    * <tt>false</tt>, this method returns the value of the entry being
    * deleted. This method is useful for situations where cache listeners
    * need to know what the old value being deleted is when getting
    * <tt>isPre()</tt> is <tt>false</tt> callbacks.
    *
    * @return the value of the entry being deleted, regardless of
    * <tt>isPre()</tt> value
    */
   V getOldValue();

   /**
    * This will be true if the write command that caused this had to be retried again due to a topology change.  This
    * could be a sign that this event has been duplicated or another event was dropped and replaced
    * (eg: ModifiedEvent replaced CreateEvent)
    * @return Whether the command that caused this event was retried
    */
   boolean isCommandRetried();
}
