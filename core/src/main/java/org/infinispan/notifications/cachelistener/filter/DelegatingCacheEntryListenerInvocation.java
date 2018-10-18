package org.infinispan.notifications.cachelistener.filter;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.encoding.DataConversion;
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
   public CompletionStage<Void> invoke(Event<K, V> event) {
      return invocation.invoke(event);
   }

   @Override
   public CompletionStage<Void> invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean isLocalNodePrimaryOwner) {
      return null;
   }

   @Override
   public CompletionStage<Void> invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean skipQueue, boolean skipConverter, boolean needsTransform) {
      return invocation.invokeNoChecks(event, skipQueue, skipConverter, needsTransform);
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
   public DataConversion getKeyDataConversion() {
      return invocation.getKeyDataConversion();
   }

   @Override
   public DataConversion getValueDataConversion() {
      return invocation.getValueDataConversion();
   }

   @Override
   public boolean useStorageFormat() {
      return invocation.useStorageFormat();
   }
}
