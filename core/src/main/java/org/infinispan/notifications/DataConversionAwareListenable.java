package org.infinispan.notifications;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.filter.KeyFilter;
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * @since 9.1
 */
public interface DataConversionAwareListenable<K, V> extends ClassLoaderAwareFilteringListenable<K, V> {

   default <C> void addListener(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader) {
      CompletionStages.join(addListenerAsync(listenerHolder, filter, converter, classLoader));
   }

   <C> CompletionStage<Void> addListenerAsync(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader);

   default <C> void addFilteredListener(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      CompletionStages.join(addFilteredListenerAsync(listenerHolder, filter, converter, filterAnnotations));
   }

   <C> CompletionStage<Void> addFilteredListenerAsync(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                Set<Class<? extends Annotation>> filterAnnotations);

   /**
    *
    * @param listenerHolder
    * @param filter
    * @param <C>
    * @deprecated Method uses KeyFilter and is no longer supported
    */
   @Deprecated
   <C> void addListener(ListenerHolder listenerHolder, KeyFilter<? super K> filter);
}
