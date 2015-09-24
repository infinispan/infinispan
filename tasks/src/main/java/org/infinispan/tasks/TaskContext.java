package org.infinispan.tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;

/**
 * TaskContext.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class TaskContext {
   private Optional<Marshaller> marshaller = Optional.empty();
   private Optional<Cache<?, ?>> cache = Optional.empty();
   private Optional<Map<String, ?>> parameters = Optional.empty();

   public TaskContext() {}

   public TaskContext marshaller(Marshaller marshaller) {
      this.marshaller = Optional.of(marshaller);
      return this;
   }

   public TaskContext cache(Cache<?, ?> cache) {
      this.cache = Optional.of(cache);
      return this;
   }

   public TaskContext parameters(Map<String, ?> parameters) {
      this.parameters = Optional.of(parameters);
      return this;
   }

   public TaskContext addParameter(String name, Object value) {
      Map<String, Object> params = (Map<String, Object>) parameters.orElseGet(() -> {
         return new HashMap<>();
      });
      params.put(name, value);

      return parameters(params);
   }

   public Optional<Marshaller> getMarshaller() {
      return marshaller;
   }

   public Optional<Cache<?, ?>> getCache() {
      return cache;
   }

   public Optional<Map<String, ?>> getParameters() {
      return parameters;
   }


}
