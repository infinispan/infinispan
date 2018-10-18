package org.infinispan.notifications;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.filter.KeyFilter;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * A Listenable that can also filter events based on key
 *
 * @author Manik Surtani
 * @since 6.0
 */
public interface FilteringListenable<K, V> extends Listenable {
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
   default <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      CompletionStages.join(addListenerAsync(listener, filter, converter));
   }

   /**
    * Adds a listener to the component.  Typically, listeners would need to be annotated with {@link org.infinispan.notifications.Listener} and
    * further to that, contain methods annotated appropriately, otherwise the listener will not be registered.
    * <p/>
    * See the {@link org.infinispan.notifications.Listener} annotation for more information.
    * <p/>
    *
    * @param listener must not be null.
    * @deprecated Method uses KeyFilter and is no longer supported
    */
   @Deprecated
   void addListener(Object listener, KeyFilter<? super K> filter);

   /**
    * Asynchronous version of {@link #addListener(Object, CacheEventFilter, CacheEventConverter)}
    * @param listener listener to add, must not be null
    * @param filter
    * @param converter
    * @param <C>
    * @return CompletionStage that when complete the listener is fully installed
    */
   <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter);

   /**
    * Registers a listener limiting the cache-entry specific events only to
    * annotations that are passed in as parameter.
    * <p/>
    * For example, if the listener passed in contains callbacks for
    * {@link CacheEntryCreated} and {@link CacheEntryModified},
    * and filtered annotations contains only {@link CacheEntryCreated},
    * then the listener will be registered only for {@link CacheEntryCreated}
    * callbacks.
    * <p/>
    * Callback filtering only applies to {@link CacheEntryCreated},
    * {@link CacheEntryModified}, {@link CacheEntryRemoved}
    * and {@link CacheEntryExpired} annotations.
    * If the listener contains other annotations, these are preserved.
    * <p/>
    * This methods enables dynamic registration of listener interests at
    * runtime without the need to create several different listener classes.
    *
    * @param listener The listener to callback upon event notifications.  Must not be null.
    * @param filter The filter to see if the notification should be sent to the listener.  Can be null.
    * @param converter The converter to apply to the entry before being sent to the listener.  Can be null.
    * @param filterAnnotations cache-entry annotations to allow listener to be registered on. Must not be null.
    * @param <C> The type of the resultant value after being converted
    */
   default <C> void addFilteredListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      CompletionStages.join(addFilteredListenerAsync(listener, filter, converter, filterAnnotations));
   }

   /**
    * Asynchronous version of {@link #addFilteredListener(Object, CacheEventFilter, CacheEventConverter, Set)}
    * @param listener listener to add, must not be null
    * @param filter
    * @param converter
    * @param filterAnnotations
    * @param <C>
    * @return CompletionStage that when complete the listener is fully installed
    */
   <C> CompletionStage<Void> addFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations);

   /**
    * Same as {@link #addFilteredListener(Object, CacheEventFilter, CacheEventConverter, Set)}, but assumes the filter
    * and/or the converter will be done in the same data format as it's stored in the cache.
    */
   default <C> void addStorageFormatFilteredListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      CompletionStages.join(addStorageFormatFilteredListenerAsync(listener, filter, converter, filterAnnotations));
   }

   /**
    * Asynchronous version of {@link #addStorageFormatFilteredListener(Object, CacheEventFilter, CacheEventConverter, Set)}
    * @param listener listener to add, must not be null
    * @param filter
    * @param converter
    * @param filterAnnotations
    * @param <C>
    * @return CompletionStage that when complete the listener is fully installed
    */
   <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                Set<Class<? extends Annotation>> filterAnnotations);

}
