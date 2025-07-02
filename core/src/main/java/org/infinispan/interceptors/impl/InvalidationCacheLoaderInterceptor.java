package org.infinispan.interceptors.impl;

import java.lang.invoke.MethodHandles;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheType;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link CacheLoaderInterceptor} extension to add {@link CacheType#INVALIDATION} specific logic to load data from
 * persistence.
 * <p>
 * With invalidation mode, all nodes must read the data from persistence (if the {@link WriteCommand} needs it) when
 * executing the command locally.
 *
 * @since 16.0
 */
public class InvalidationCacheLoaderInterceptor<K, V> extends CacheLoaderInterceptor<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      // no ownership checking for invalidation mode as every node is an owner.
      boolean skip = hasSkipLoadFlag(cmd) ||
            cmd.loadType() == VisitableCommand.LoadType.DONT_LOAD;
      if (log.isTraceEnabled() && skip) {
         log.tracef("Skip load for command %s.", cmd);
      }

      return skip;
   }

}
