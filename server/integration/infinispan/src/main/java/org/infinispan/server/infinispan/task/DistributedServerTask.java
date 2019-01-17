package org.infinispan.server.infinispan.task;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskContext;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/28/16
 * Time: 1:49 PM
 */
public class DistributedServerTask<T> implements Serializable, Function<EmbeddedCacheManager, T> {
   private final String cacheName;
   private final Optional<Map<String, ?>> parameters;
   private final String taskName;

   public DistributedServerTask(String cacheName, String taskName, Optional<Map<String, ?>> parameters) {
      this.cacheName = cacheName;
      this.taskName = taskName;
      this.parameters = parameters;
   }

   @Override
   public T apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = embeddedCacheManager.getCache(cacheName);
      // todo inject global component registry to be independent of existence of cache.
      GlobalComponentRegistry componentRegistry = cache.getCacheManager().getGlobalComponentRegistry();
      ServerTaskRegistry taskRegistry = componentRegistry.getComponent(ServerTaskRegistry.class);
      Marshaller marshaller = componentRegistry.getComponent(StreamingMarshaller.class);
      ServerTaskWrapper<T> task = taskRegistry.getTask(taskName);
      task.inject(prepareContext(cache, marshaller));
      try {
         return task.run();
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private TaskContext prepareContext(Cache<Object, Object> cache, Marshaller marshaller) {
      TaskContext context = new TaskContext();
      if (parameters.isPresent()) context.parameters(parameters.get());
      String type = MediaType.APPLICATION_OBJECT_TYPE;
      if (cache != null) context.cache(cache.getAdvancedCache().withMediaType(type, type));
      if (marshaller != null) context.marshaller(marshaller);

      return context;
   }
}
