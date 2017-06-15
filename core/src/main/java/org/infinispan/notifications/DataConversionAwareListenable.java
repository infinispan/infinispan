package org.infinispan.notifications;

import java.lang.annotation.Annotation;
import java.util.Set;

import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;

/**
 * @since 9.1
 */
public interface DataConversionAwareListenable<K, V> extends ClassLoaderAwareFilteringListenable<K, V> {

   <C> void addListener(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, ClassLoader classLoader);

   <C> void addFilteredListener(ListenerHolder listenerHolder, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                Set<Class<? extends Annotation>> filterAnnotations);
}
