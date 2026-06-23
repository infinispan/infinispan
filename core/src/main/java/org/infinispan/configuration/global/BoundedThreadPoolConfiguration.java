package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * @since 10.0
 */
class BoundedThreadPoolConfiguration extends ConfigurationElement<BoundedThreadPoolConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> THREAD_FACTORY = AttributeDefinition.builder("threadFactory", null, String.class).build();
   static final AttributeDefinition<Integer> MAX_THREADS = AttributeDefinition.builder("maxThreads", null, Integer.class).build();
   static final AttributeDefinition<Integer> CORE_THREADS = AttributeDefinition.builder("coreThreads", null, Integer.class).build();
   static final AttributeDefinition<Long> KEEP_ALIVE_TIME = AttributeDefinition.builder("keepAliveTime", null, Long.class).build();
   static final AttributeDefinition<Integer> QUEUE_LENGTH = AttributeDefinition.builder("queue-length", null, Integer.class).build();
   static final AttributeDefinition<Boolean> NON_BLOCKING = AttributeDefinition.builder("non-blocking", false, Boolean.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(BoundedThreadPoolConfiguration.class, NAME, THREAD_FACTORY, MAX_THREADS, CORE_THREADS,
            KEEP_ALIVE_TIME, QUEUE_LENGTH, NON_BLOCKING);
   }

   BoundedThreadPoolConfiguration(AttributeSet attributes) {
      super(attributes.attribute(NON_BLOCKING).get() ? Element.NON_BLOCKING_BOUNDED_QUEUE_THREAD_POOL : Element.BLOCKING_BOUNDED_QUEUE_THREAD_POOL, attributes);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String threadFactory() {
      return attributes.attribute(THREAD_FACTORY).get();
   }

   public Integer getMaxThreads() {
      return attributes.attribute(MAX_THREADS).get();
   }

   public Integer getCoreThreads() {
      return attributes.attribute(CORE_THREADS).get();
   }

   public Long getKeepAliveTime() {
      return attributes.attribute(KEEP_ALIVE_TIME).get();
   }

   public Integer getQueueLength() {
      return attributes.attribute(QUEUE_LENGTH).get();
   }

   public Boolean isNonBlocking() {
      return attributes.attribute(NON_BLOCKING).get();
   }
}
