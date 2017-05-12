package org.infinispan.counter.impl.interceptor;

import java.util.Collection;
import java.util.EnumSet;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.metadata.Metadata;

/**
 * Interceptor for the counters cache.
 * <p>
 * Since the state transfer doesn't know about the {@link Flag#SKIP_CACHE_STORE} and {@link Flag#SKIP_CACHE_LOAD} flags,
 * all the counters are persisted. However, we only want the {@link Storage#PERSISTENT} configurations to be persisted.
 * <p>
 * This interceptor checks the configuration's {@link Storage} and sets the {@link Flag#SKIP_CACHE_LOAD} and {@link
 * Flag#SKIP_CACHE_STORE} flags.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterInterceptor extends BaseCustomAsyncInterceptor {

   private static final Log log = LogFactory.getLog(CounterInterceptor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Collection<Flag> FLAGS_TO_SKIP_PERSISTENCE = EnumSet
         .of(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE);

   private static ConfigurationMetadata extract(Metadata metadata) {
      return metadata instanceof MetaParamsInternalMetadata ?
            ((MetaParamsInternalMetadata) metadata).findMetaParam(ConfigurationMetadata.class).orElse(null) :
            null;
   }

   private static boolean isVolatile(ConfigurationMetadata metadata) {
      return metadata != null && metadata.get().storage() == Storage.VOLATILE;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      //State Transfer puts doesn't use the skip_cache_load/store and the volatile counters are stored.
      //interceptor should be between the entry wrapping and the cache loader/writer interceptors.
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      ConfigurationMetadata entryMetadata = entry == null ? null : extract(entry.getMetadata());
      ConfigurationMetadata commandMetadata = extract(command.getMetadata());
      if (isVolatile(entryMetadata) || isVolatile(commandMetadata)) {
         if (trace) {
            log.tracef("Setting skip persistence for %s", command.getKey());
         }
         command.setFlagsBitSet(EnumUtil.setEnums(command.getFlagsBitSet(), FLAGS_TO_SKIP_PERSISTENCE));
      }
      return invokeNext(ctx, command);
   }
}
