package org.infinispan.server.iteration;

import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheStream;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.stream.impl.local.LocalCacheStream;

/**
 * A specialized implementation of {@link DefaultIterationManager} that is used when the source of the iteration is
 * external to the cache.
 * <p>
 * The base stream is extracted from the {@link IterationInitializationContext} and wrapped in a {@link LocalCacheStream}.
 * This makes it possible to still apply filters and converters to the stream. If no context is provided, defaults to
 * an empty stream.
 *
 * @since 15.0
 * @see DefaultIterationManager
 */
public class ExternalSourceIterationManager extends DefaultIterationManager {

   private static final CacheStream<CacheEntry<Object, Object>> EMPTY_STREAM =
         new LocalCacheStream<>((is, s, b) -> Stream.empty(), false, null);

   public ExternalSourceIterationManager(TimeService timeService) {
      super(timeService);
   }

   @Override
   protected CacheStream<CacheEntry<Object, Object>> baseStream(AdvancedCache cache, IterationInitializationContext ctx) {
      return ctx == null
            ? EMPTY_STREAM
            : new LocalCacheStream<>(ctx.getBaseStream(), false, SecurityActions.getCacheComponentRegistry(cache));
   }
}
