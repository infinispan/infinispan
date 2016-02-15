package org.infinispan.tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;

/**
 * TaskContext. Defines the execution context of a task by specifying parameters, cache and
 * marshaller
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class TaskContext {
   private Optional<Marshaller> marshaller = Optional.empty();
   private Optional<Cache<?, ?>> cache = Optional.empty();
   private Optional<Map<String, ?>> parameters = Optional.empty();

   public TaskContext() {
   }

   /**
    * The marshaller with which this task should be executed
    */
   public TaskContext marshaller(Marshaller marshaller) {
      this.marshaller = Optional.of(marshaller);
      return this;
   }

   /**
    * The cache against which this task will be executed. This will be the task's default cache, but
    * other caches can be obtained from the cache manager
    */
   public TaskContext cache(Cache<?, ?> cache) {
      this.cache = Optional.of(cache);
      return this;
   }

   /**
    * A map of named parameters that will be passed to the task. Invoking this method overwrites any
    * previously set parameters
    */
   public TaskContext parameters(Map<String, ?> parameters) {
      this.parameters = Optional.of(parameters);
      return this;
   }

   /**
    * Adds a named parameter to the task context
    */
   public TaskContext addParameter(String name, Object value) {
      Map<String, Object> params = (Map<String, Object>) parameters.orElseGet(() -> {
         return new HashMap<>();
      });
      params.put(name, value);

      return parameters(params);
   }

   /**
    * Marshaller for this task execution
    * @return optional marshaller
    */
   public Optional<Marshaller> getMarshaller() {
      return marshaller;
   }

   /**
    * The default cache. Other caches can be obtained from cache manager ({@link Cache#getCacheManager()})
    * @return optional cache
    */
   public Optional<Cache<?, ?>> getCache() {
      return cache;
   }

   /**
    * Gets a map of named parameters for the task
    * @return optional map of named parameters for the task
    */
   public Optional<Map<String, ?>> getParameters() {
      return parameters;
   }

}
