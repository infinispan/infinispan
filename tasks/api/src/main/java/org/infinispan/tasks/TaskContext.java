package org.infinispan.tasks;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.security.Security;

/**
 * TaskContext. Defines the execution context of a task by specifying parameters, cache and marshaller
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@ProtoTypeId(ProtoStreamTypeIds.DISTRIBUTED_SERVER_TASK_CONTEXT)
public class TaskContext {
   private transient EmbeddedCacheManager cacheManager;
   private transient Marshaller marshaller;
   private transient Cache<?, ?> cache;
   private Map<String, Object> parameters = Collections.emptyMap();
   private Subject subject;
   private transient boolean logEvent;

   public TaskContext() {
   }

   public TaskContext(TaskContext other) {
      this.parameters = other.parameters;
      this.subject = other.subject;
   }

   @ProtoFactory
   TaskContext(Collection<TaskParameter> parameters, Subject subject) {
      this.parameters = parameters.stream().collect(Collectors.toMap(p -> p.key, p -> p.value));
      this.subject = subject;
   }

   /**
    * The cache manager with which this task should be executed
    */
   public TaskContext cacheManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      return this;
   }

   /**
    * The marshaller with which this task should be executed
    */
   public TaskContext marshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
      return this;
   }

   /**
    * The cache against which this task will be executed. This will be the task's default cache, but other caches can be
    * obtained from the cache manager
    */
   public TaskContext cache(Cache<?, ?> cache) {
      this.cache = cache;
      return this;
   }

   /**
    * A map of named parameters that will be passed to the task. Invoking this method overwrites any previously set
    * parameters
    */
   public TaskContext parameters(Map<String, ?> parameters) {
      this.parameters = (Map<String, Object>) parameters;
      return this;
   }

   /**
    * The subject to impersonate when running this task. If unspecified, the Subject (if any) will be retrieved via
    * {@link Security#getSubject()}
    */
   public TaskContext subject(Subject subject) {
      this.subject = subject;
      return this;
   }

   /**
    * Adds a named parameter to the task context
    */
   public TaskContext addParameter(String name, Object value) {
      if (parameters == Collections.EMPTY_MAP) {
         parameters = new HashMap<>();
      }
      parameters.put(name, value);
      return this;
   }

   /**
    * Adds a named parameter to the task context only if it is non-null
    */
   public TaskContext addOptionalParameter(String name, Object value) {
      if (value != null) {
         return addParameter(name, value);
      } else {
         return this;
      }
   }

   /**
    * Whether execution will generate an event in the event log
    */
   public TaskContext logEvent(boolean logEvent) {
      this.logEvent = logEvent;
      return this;
   }

   /**
    * CacheManager for this task execution
    *
    * @return the cache manager
    */
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   /**
    * Marshaller for this task execution
    *
    * @return optional marshaller
    */
   public Optional<Marshaller> getMarshaller() {
      return Optional.ofNullable(marshaller);
   }

   /**
    * The default cache. Other caches can be obtained from cache manager ({@link Cache#getCacheManager()})
    *
    * @return optional cache
    */
   public Optional<Cache<?, ?>> getCache() {
      return Optional.ofNullable(cache);
   }

   /**
    * Gets a map of named parameters for the task
    *
    * @return optional map of named parameters for the task
    */
   public Optional<Map<String, Object>> getParameters() {
      return Optional.of(parameters);
   }

   /**
    * The optional {@link Subject} which is executing this task
    *
    * @return the {@link Subject}
    */
   public Optional<Subject> getSubject() {
      return Optional.ofNullable(subject);
   }

   /**
    * Whether executing this task will generate an event in the event log
    *
    * @return true if an event will be logged, false otherwise
    */
   public boolean isLogEvent() {
      return logEvent;
   }

   @Override
   public String toString() {
      return "TaskContext{" +
            "marshaller=" + marshaller +
            ", cache=" + cache +
            ", parameters=" + parameters +
            ", subject=" + subject +
            ", logEvent=" + logEvent +
            '}';
   }

   @ProtoField(1)
   Collection<TaskParameter> parameters() {
      return parameters.entrySet().stream().map(e -> new TaskParameter(e.getKey(), e.getValue().toString())).collect(Collectors.toList());
   }

   @ProtoField(2)
   public Subject subject() {
      return subject;
   }

   @ProtoTypeId(ProtoStreamTypeIds.DISTRIBUTED_SERVER_TASK_PARAMETER)
   static class TaskParameter {
      @ProtoField(1)
      String key;

      @ProtoField(2)
      String value;

      @ProtoFactory
      TaskParameter(String key, String value) {
         this.key = key;
         this.value = value;
      }
   }
}
