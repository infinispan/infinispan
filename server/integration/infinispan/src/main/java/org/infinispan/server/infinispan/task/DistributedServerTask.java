package org.infinispan.server.infinispan.task;

import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.tasks.TaskContext;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/28/16
 * Time: 1:49 PM
 */
public class DistributedServerTask<T> implements Serializable, Function<EmbeddedCacheManager, T> {
   private final String cacheName;
   private final String taskName;
   private final TaskContext context;

   public DistributedServerTask(String cacheName, String taskName, TaskContext context) {
      this.cacheName = cacheName;
      this.taskName = taskName;
      this.context = context;
   }

   @Override
   public T apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = embeddedCacheManager.getCache(cacheName);
      GlobalComponentRegistry componentRegistry = SecurityActions.getGlobalComponentRegistry(embeddedCacheManager);
      ServerTaskRegistry taskRegistry = componentRegistry.getComponent(ServerTaskRegistry.class);
      Marshaller marshaller = componentRegistry.getComponent(StreamingMarshaller.class);
      ServerTaskWrapper<T> task = taskRegistry.getTask(taskName);
      TaskContext taskContext = prepareContext(cache, marshaller);
      task.inject(taskContext);
      try {
         if (taskContext.getSubject().isPresent()) {
            return Security.doAs(taskContext.getSubject().get(), (PrivilegedExceptionAction<T>) task::run);
         } else {
            return task.run();
         }
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private TaskContext prepareContext(Cache<Object, Object> cache, Marshaller marshaller) {
      TaskContext context = new TaskContext(this.context);
      String type = MediaType.APPLICATION_OBJECT_TYPE;
      if (cache != null) context.cache(cache.getAdvancedCache().withMediaType(type, type));
      if (marshaller != null) context.marshaller(marshaller);
      return context;
   }
}
