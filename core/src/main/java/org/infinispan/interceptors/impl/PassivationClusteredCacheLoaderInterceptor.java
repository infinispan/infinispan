package org.infinispan.interceptors.impl;

import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationClusteredCacheLoaderInterceptor<K, V> extends ClusteredCacheLoaderInterceptor<K, V> {
   private static final Log log = LogFactory.getLog(PassivationClusteredCacheLoaderInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject
   @ComponentName(PERSISTENCE_EXECUTOR)
   ExecutorService persistenceExecutor;

   @Override
   protected CompletionStage<Void> loadInContext(InvocationContext ctx, Object key, FlagAffectedCommand cmd) {
      return PassivationCacheLoaderInterceptor.asyncLoad(persistenceExecutor, this, key, cmd, ctx);
   }
}
