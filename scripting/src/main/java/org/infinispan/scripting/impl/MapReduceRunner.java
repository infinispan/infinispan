package org.infinispan.scripting.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * MapReduceRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 * @deprecated Use the streaming API within a local script instead
 */
@Deprecated
public class MapReduceRunner implements ScriptRunner {
   public static final MapReduceRunner INSTANCE = new MapReduceRunner();

   private MapReduceRunner() {
   }

   @Override
   public <T> CompletableFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding) {
      MapReduceTask mapReduceTask = new MapReduceTask<>((Cache<?, ?>) binding.get(SystemBindings.CACHE.toString()));
      mapReduceTask.mappedWith(new MapperScript<>(metadata));
      metadata.reducer().ifPresent((reducer) -> {
         mapReduceTask.reducedWith(new ReducerScript<>(scriptManager.getScriptMetadata(reducer)));
      });
      metadata.combiner().ifPresent((combiner) -> {
         mapReduceTask.combinedWith(new ReducerScript<>(scriptManager.getScriptMetadata(combiner)));
      });

      Future<T> future = metadata.collator().map((collator) -> {
         return mapReduceTask.executeAsynchronously(new CollatorScript<>(scriptManager.getScriptMetadata(collator), scriptManager));
      }).orElseGet(() -> {
         return mapReduceTask.executeAsynchronously();
      });

      return CompletableFutures.toCompletableFuture((NotifyingFuture<T>) future);
   }

}
