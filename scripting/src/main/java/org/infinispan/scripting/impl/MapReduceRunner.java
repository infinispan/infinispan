package org.infinispan.scripting.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
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
      String reducerScript = metadata.reducer();
      if (reducerScript != null) {
         mapReduceTask.reducedWith(new ReducerScript<>(scriptManager.getScriptMetadata(reducerScript)));
      }
      String combinerScript = metadata.combiner();
      if (combinerScript != null) {
         mapReduceTask.combinedWith(new ReducerScript<>(scriptManager.getScriptMetadata(combinerScript)));
      }
      String collatorScript = metadata.collator();
      NotifyingFutureImpl<T> future;
      if (collatorScript != null) {
         future = (NotifyingFutureImpl<T>) mapReduceTask.executeAsynchronously(new CollatorScript<>(scriptManager.getScriptMetadata(collatorScript), scriptManager));
      } else {
         future = (NotifyingFutureImpl<T>) mapReduceTask.executeAsynchronously();
      }
      return CompletableFutures.connect(future);
   }

}
