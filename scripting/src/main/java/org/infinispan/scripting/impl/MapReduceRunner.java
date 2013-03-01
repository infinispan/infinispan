package org.infinispan.scripting.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.scripting.impl.ScriptMetadata.MetadataProperties;

/**
 * MapReduceRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class MapReduceRunner implements ScriptRunner {
   public static final MapReduceRunner INSTANCE = new MapReduceRunner();

   private MapReduceRunner() {
   }

   @Override
   public <T> NotifyingFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding) {
      MapReduceTask mapReduceTask = new MapReduceTask<>((Cache<?, ?>) binding.get(SystemBindings.CACHE.toString()));
      mapReduceTask.mappedWith(new MapperScript<>(metadata));
      String reducerScript = metadata.property(MetadataProperties.REDUCER);
      if (reducerScript != null) {
         mapReduceTask.reducedWith(new ReducerScript<>(scriptManager.getScriptMetadata(reducerScript)));
      }
      String combinerScript = metadata.property(MetadataProperties.COMBINER);
      if (combinerScript != null) {
         mapReduceTask.combinedWith(new ReducerScript<>(scriptManager.getScriptMetadata(combinerScript)));
      }
      String collatorScript = metadata.property(MetadataProperties.COLLATOR);
      if (collatorScript != null) {
         return (NotifyingFuture<T>) mapReduceTask.executeAsynchronously(new CollatorScript<>(scriptManager.getScriptMetadata(collatorScript), scriptManager));
      } else {
         return (NotifyingFuture<T>) mapReduceTask.executeAsynchronously();
      }
   }

}
