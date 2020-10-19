package org.infinispan.server.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.tasks.TaskContext;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
@ProtoTypeId(ProtoStreamTypeIds.DISTRIBUTED_SERVER_TASK)
public class DistributedServerTask<T> implements Function<EmbeddedCacheManager, T> {
   @ProtoField(1)
   final String taskName;

   @ProtoField(2)
   final String cacheName;

   @ProtoField(number = 3, collectionImplementation = ArrayList.class)
   final List<TaskParameter> parameters;

   @ProtoFactory
   public DistributedServerTask(String taskName, String cacheName, List<TaskParameter> parameters) {
      this.cacheName = cacheName;
      this.taskName = taskName;
      this.parameters = parameters;
   }

   @Override
   public T apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = embeddedCacheManager.getCache(cacheName);
      // todo inject global component registry to be independent of existence of cache.
      GlobalComponentRegistry componentRegistry = SecurityActions.getGlobalComponentRegistry(embeddedCacheManager);
      ServerTaskEngine serverTaskEngine = componentRegistry.getComponent(ServerTaskEngine.class);
      Marshaller marshaller = componentRegistry.getComponent(StreamingMarshaller.class);
      ServerTaskWrapper<T> task = serverTaskEngine.getTask(taskName);
      task.inject(prepareContext(embeddedCacheManager, cache, marshaller));
      try {
         return task.run();
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private TaskContext prepareContext(EmbeddedCacheManager embeddedCacheManager, Cache<Object, Object> cache, Marshaller marshaller) {
      TaskContext context = new TaskContext();
      context.cacheManager(embeddedCacheManager);
      Map<String, String> params = parameters.stream().collect(Collectors.toMap(p -> p.key, p -> p.value));
      context.parameters(params);
      MediaType type = MediaType.APPLICATION_OBJECT;
      if (cache != null) context.cache(cache.getAdvancedCache().withMediaType(type, type));
      if (marshaller != null) context.marshaller(marshaller);

      return context;
   }
}
