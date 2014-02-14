package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryModified}
 * <p />
 * The {@link #getValue()} method's behavior is specific to whether the callback is triggered before or after the event
 * in question.  For example, if <tt>event.isPre()</tt> is <tt>true</tt>, then <tt>event.getValue()</tt> would return the
 * <i>old</i> value, prior to modification.  If <tt>event.isPre()</tt> is <tt>false</tt>, then <tt>event.getValue()</tt>
 * would return new <i>new</i> value.  If the event is creating and inserting a new entry, the old value would be <tt>null</tt>.
 * <p />
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryModifiedEvent<K, V> extends CacheEntryEvent<K, V> {

   /**
    * Retrieves the value of the entry being modified.
    * <p />
    * @return the previous or new value of the entry, depending on whether isPre() is true or false.
    */
   V getValue();

   /**
    * Indicates whether the cache entry modification event is the result of
    * the cache entry being created. This method helps determine if the cache
    * entry was created when <tt>event.isPre()</tt> is <tt>false</tt>.
    *
    * @return true if the event is the result of the entry being created,
    * otherwise it returns false indicating that the event was the result of
    * a cache entry being updated
    */
   boolean isCreated();

   /**
    * This will be true if the write command that caused this had to be retried again due to a topology change.  This
    * could be a sign that this event has been duplicated or another event was dropped and replaced
    * (eg: ModifiedEvent replaced CreateEvent)
    * @return Whether the command that caused this event was retried
    */
   boolean isCommandRetried();

}
