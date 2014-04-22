package org.infinispan.notifications;

import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;

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
    * @param listener The listener to callback upon event notifications.  Must not be null.
    * @param filter The filter to see if the notification should be sent to the listener.  Can be null.
    * @param converter The converter to apply to the entry before being sent to the listener.  Can be null.
    * @param <C> The type of the resultant value after being converted
    * @throws NullPointerException if the specified listener is null
    */
   <C> void addListener(Object listener, KeyValueFilter<? super K, ? super V> filter, Converter<? super K, ? super V, C> converter);
}
