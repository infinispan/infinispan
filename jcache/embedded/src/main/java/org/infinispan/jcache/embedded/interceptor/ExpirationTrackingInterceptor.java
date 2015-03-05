package org.infinispan.jcache.embedded.interceptor;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jcache.AbstractJCacheNotifier;
import org.infinispan.util.TimeService;

import javax.cache.Cache;

/**
 * An interceptor that tracks expiration of entries and notifies JCache
 * {@link javax.cache.event.CacheEntryExpiredListener} instances.
 *
 * This interceptor must be placed before
 * {@link org.infinispan.interceptors.EntryWrappingInterceptor} because this
 * interceptor can result in container entries being removed upon expiration
 * (alongside their metadata).
 *
 * TODO: How to track expired entry in cache stores?
 * TODO: Could this be used as starting point to centrally track expiration?
 * Currently, logic split between data container, cache stores...etc.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class ExpirationTrackingInterceptor extends CommandInterceptor {

   private final DataContainer container;
   private final Cache<Object, Object> cache;
   private final AbstractJCacheNotifier<Object, Object> notifier;
   private final TimeService timeService;

   @SuppressWarnings("unchecked")
   public ExpirationTrackingInterceptor(DataContainer container,
         Cache<?, ?> cache, AbstractJCacheNotifier<?, ?> notifier, TimeService timeService) {
      this.container = container;
      this.timeService = timeService;
      this.cache = (Cache<Object, Object>) cache;
      this.notifier = (AbstractJCacheNotifier<Object, Object>) notifier;
   }

   @Override
   public Object visitGetKeyValueCommand
         (InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      InternalCacheEntry entry = container.peek(key);
      if (entry != null && entry.canExpire() && entry.isExpired(timeService.wallClockTime()))
         notifier.notifyEntryExpired(cache, key, entry.getValue());

      return super.visitGetKeyValueCommand(ctx, command);
   }

   // TODO: Implement any other visitX methods?

}
