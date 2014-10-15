package org.infinispan.notifications;

import org.infinispan.filter.KeyFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;

/**
 * A Listable that can also filter events based on key
 *
 * @author Manik Surtani
 * @since 6.0
 */
public interface FilteringListenable<K, V> extends Listenable {
   /**
    * Adds a listener to the component.  Typically, listeners would need to be annotated with {@link org.infinispan.notifications.Listener} and
    * further to that, contain methods annotated appropriately, otherwise the listener will not be registered.
    * <p/>
    * See the {@link org.infinispan.notifications.Listener} annotation for more information.
    * <p/>
    *
    * @param listener must not be null.
    */
   void addListener(Object listener, KeyFilter<? super K> filter);

   /**
    * Registers a listener that will be notified on events that pass the filter condition.  The value presented in the
    * notifications will be first converted using the provided converter if there is one.
    * <p>
    * Some implementations may provide optimizations when a
    * {@link org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter} is provided as both arguments
    * to the <b>filter</b> and <b>converter</b> arguments.  Note the provided object must have reference equality ie. (==)
    * to be recognized.  This allows for the filter and conversion step to take place in the same method call reducing
    * possible overhead.
    * @param listener The listener to callback upon event notifications.  Must not be null.
    * @param filter The filter to see if the notification should be sent to the listener.  Can be null.
    * @param converter The converter to apply to the entry before being sent to the listener.  Can be null.
    * @param <C> The type of the resultant value after being converted
    * @throws NullPointerException if the specified listener is null
    */
   <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter);
}
