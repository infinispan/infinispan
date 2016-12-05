package org.infinispan.counter.impl.interceptor;

import static java.util.EnumSet.of;

import java.util.Collection;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.logging.Log;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;

/**
 * Interceptor for the counters configuration cache.
 * <p>
 * Since the state transfer doesn't know about the {@link Flag#SKIP_CACHE_STORE} and {@link Flag#SKIP_CACHE_LOAD} flags,
 * all  the configuration are persisted. However, we only want the {@link Storage#PERSISTENT} configurations to be
 * persisted.
 * <p>
 * This interceptor checks the configuration's {@link Storage} and sets the {@link Flag#SKIP_CACHE_LOAD} and {@link
 * Flag#SKIP_CACHE_STORE} flags.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterConfigurationInterceptor extends BaseCustomAsyncInterceptor {

   private static final Log log = LogFactory.getLog(CounterConfigurationInterceptor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Collection<Flag> FLAGS_TO_SKIP_PERSISTENCE = of(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE);

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      //noinspection unchecked
      CacheEntry<String, CounterConfiguration> entry = ctx.lookupEntry(command.getKey());
      CounterConfiguration value = entry == null ? null : entry.getValue();
      if (value != null && value.storage() == Storage.VOLATILE) {
         if (trace) {
            log.tracef("Setting skip persistence for %s", command.getKey());
         }
         command.setFlagsBitSet(EnumUtil.setEnums(command.getFlagsBitSet(), FLAGS_TO_SKIP_PERSISTENCE));
      }
      return invokeNext(ctx, command);
   }
}
