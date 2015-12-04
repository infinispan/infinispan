package org.infinispan.scripting.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;

/**
 * DistributedRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class DistributedRunner implements ScriptRunner {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   public static final DistributedRunner INSTANCE = new DistributedRunner();

   private DistributedRunner() {
   }

   @Override
   public <T> CompletableFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding) {
      Cache<?, ?> masterCacheNode = (Cache<?, ?>) binding.get(SystemBindings.CACHE.toString());
      if (masterCacheNode == null) {
         throw log.distributedTaskNeedCacheInBinding(metadata.name());
      }
      DefaultExecutorService des = new DefaultExecutorService(masterCacheNode);
      try {
         List<Future<T>> tasks = des.submitEverywhere(new DistributedScript<T>(metadata));
         List<CompletableFuture<T>> all = new ArrayList<>(tasks.size());
         tasks.forEach(task -> all.add(CompletableFutures.toCompletableFuture((NotifyingFuture<T>) task)));

         return (CompletableFuture<T>) CompletableFutures.sequence(all);
      } finally {
         des.shutdown();
      }
   }
}
