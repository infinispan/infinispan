package org.infinispan.notifications;

import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;

/**
 * Interface that enhances {@link FilteringListenable} with the possibility of specifying the
 * {@link ClassLoader} which should be set as the context class loader for the invoked
 * listener method
 *
 * @author Manik Surtani
 * @since 6.0
 * @see ClassLoaderAwareListenable
 * @see FilteringListenable
 */
public interface ClassLoaderAwareFilteringListenable<K, V> extends FilteringListenable<K, V> {
   /**
    * Adds a listener to the component.  Typically, listeners would need to be annotated with {@link org.infinispan.notifications.Listener} and
    * further to that, contain methods annotated appropriately, otherwise the listener will not be registered.
    * <p/>
    * See the {@link org.infinispan.notifications.Listener} annotation for more information.
    * <p/>
    *
    * @param listener must not be null.
    * @param classLoader class loader
    */
   void addListener(Object listener, KeyFilter<? super K> filter, ClassLoader classLoader);

   /**
    * Adds a listener with the provided filter and converter and using a given classloader when invoked.  See
    * {@link org.infinispan.notifications.FilteringListenable#addListener(Object, org.infinispan.filter.KeyValueFilter, org.infinispan.filter.Converter)}
    * for more details.
    * <p/>
    * @param listener must not be null.  The listener to callback on when an event is raised
    * @param filter The filter to apply for the entry to see if the event should be raised
    * @param converter The converter to convert the filtered entry to a new value
    * @param classLoader The class loader to use when the event is fired
    * @param <C> The type that the converter returns.  The listener must handle this type in any methods that handle
    *            events being returned
    */
   <C> void addListener(Object listener, KeyValueFilter<? super K, ? super V> filter,
                    Converter<? super K, ? super V, C> converter, ClassLoader classLoader);
}
