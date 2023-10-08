package org.infinispan.server.tasks;

import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.GlobalMarshaller;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.security.actions.SecurityActions;
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

   @ProtoField(3)
   final TaskContext context;

   @ProtoFactory
   public DistributedServerTask(String taskName, String cacheName, TaskContext context) {
      this.cacheName = cacheName;
      this.taskName = taskName;
      this.context = context;
   }

   @Override
   public T apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = cacheName != null ? embeddedCacheManager.getCache(cacheName) : null;
      // todo inject global component registry to be independent of existence of cache.
      GlobalComponentRegistry componentRegistry = SecurityActions.getGlobalComponentRegistry(embeddedCacheManager);
      ServerTaskEngine serverTaskEngine = componentRegistry.getComponent(ServerTaskEngine.class);
      Marshaller marshaller = componentRegistry.getComponent(GlobalMarshaller.class);
      ServerTaskWrapper<T> task = serverTaskEngine.getTask(taskName);
      TaskContext ctx = prepareContext(embeddedCacheManager, cache, marshaller);
      try {
         return task.run(ctx);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private TaskContext prepareContext(EmbeddedCacheManager embeddedCacheManager, Cache<Object, Object> cache, Marshaller marshaller) {
      TaskContext context = new TaskContext(this.context);
      context.cacheManager(embeddedCacheManager);
      MediaType type = MediaType.APPLICATION_OBJECT;
      if (cache != null) context.cache(cache.getAdvancedCache().withMediaType(type, type));
      if (marshaller != null) context.marshaller(marshaller);
      return context;
   }
}
