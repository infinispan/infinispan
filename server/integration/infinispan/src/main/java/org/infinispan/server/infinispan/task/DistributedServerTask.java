package org.infinispan.server.infinispan.task;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.tasks.TaskContext;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/28/16
 * Time: 1:49 PM
 */
public class DistributedServerTask<T> implements Serializable, DistributedCallable<Object, Object, T> {
   private final Optional<Map<String, ?>> parameters;
   private final String taskName;

   private transient ServerTaskRegistry taskRegistry;
   private transient Marshaller marshaller;
   private transient Cache<Object, Object> cache;

   public DistributedServerTask(String taskName, Optional<Map<String, ?>> parameters) {
      this.taskName = taskName;
      this.parameters = parameters;
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      this.cache = cache;
      // todo inject global component registry to be independent of existence of cache.
      GlobalComponentRegistry componentRegistry = cache.getCacheManager().getGlobalComponentRegistry();
      taskRegistry = componentRegistry.getComponent(ServerTaskRegistry.class);
      marshaller = componentRegistry.getComponent(StreamingMarshaller.class);
   }

   @Override
   public T call() throws Exception {
      ServerTaskWrapper<T> task = taskRegistry.getTask(taskName);
      task.inject(prepareContext());
      return task.run();
   }

   private TaskContext prepareContext() {
      TaskContext context = new TaskContext();
      if (parameters.isPresent()) context.parameters(parameters.get());
      String type = MediaType.APPLICATION_OBJECT_TYPE;
      if (cache != null) context.cache(cache.getAdvancedCache().withMediaType(type, type));
      if (marshaller != null) context.marshaller(marshaller);

      return context;
   }
}
