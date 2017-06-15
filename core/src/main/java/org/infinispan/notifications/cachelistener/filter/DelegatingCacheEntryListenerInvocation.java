package org.infinispan.notifications.cachelistener.filter;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheEntryListenerInvocation;
import org.infinispan.notifications.cachelistener.EventWrapper;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;

/**
 * A wrapper around a {@link CacheEntryListenerInvocation} that keeps a reference to the {@link
 * FilterIndexingServiceProvider} instance that handles this invocation. All methods are delegated to the wrapped
 * invocation except {@link CacheEntryListenerInvocation#invoke(EventWrapper, boolean)} and {@link
 * CacheEntryListenerInvocation#invoke(Object)}. FilterIndexingServiceProvider implementors must extends this class and
 * implement its abstract {@link #unregister} method.
 *
 * @param <K> cache key type
 * @param <V> cache value type
 * @author anistor@redhat.com
 * @since 7.2
 */
public abstract class DelegatingCacheEntryListenerInvocation<K, V> implements CacheEntryListenerInvocation<K, V> {

   protected final CacheEntryListenerInvocation<K, V> invocation;

   protected DelegatingCacheEntryListenerInvocation(CacheEntryListenerInvocation<K, V> invocation) {
      this.invocation = invocation;
   }

   /**
    * Stops handling the invocation. This is called when the listener is being unregistered.
    */
   public abstract void unregister();

   @Override
   public Object getTarget() {
      return invocation.getTarget();
   }

   @Override
   public void invoke(Event<K, V> event) {
   }

   @Override
   public void invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean isLocalNodePrimaryOwner) {
   }

   @Override
   public void invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean skipQueue, boolean skipConverter) {
      invocation.invokeNoChecks(event, skipQueue, skipConverter);
   }

   @Override
   public boolean isClustered() {
      return invocation.isClustered();
   }

   @Override
   public boolean isSync() {
      return invocation.isSync();
   }

   @Override
   public UUID getIdentifier() {
      return invocation.getIdentifier();
   }

   @Override
   public Listener.Observation getObservation() {
      return invocation.getObservation();
   }

   @Override
   public Class<? extends Annotation> getAnnotation() {
      return invocation.getAnnotation();
   }

   @Override
   public CacheEventFilter<? super K, ? super V> getFilter() {
      return invocation.getFilter();
   }

   @Override
   public <C> CacheEventConverter<? super K, ? super V, C> getConverter() {
      return invocation.getConverter();
   }

   @Override
   public Set<Class<? extends Annotation>> getFilterAnnotations() {
      return invocation.getFilterAnnotations();
   }

   @Override
   public Encoder getKeyEncoder() {
      return invocation.getKeyEncoder();
   }

   @Override
   public Encoder getValueEncoder() {
      return invocation.getValueEncoder();
   }

   @Override
   public Wrapper getValueWrapper() {
      return invocation.getValueWrapper();
   }

   @Override
   public Wrapper getKeyWrapper() {
      return invocation.getKeyWrapper();
   }
}
