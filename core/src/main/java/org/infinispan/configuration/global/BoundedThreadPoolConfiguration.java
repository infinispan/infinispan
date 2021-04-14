package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
class BoundedThreadPoolConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> THREAD_FACTORY = AttributeDefinition.builder("threadFactory", null, String.class).build();
   static final AttributeDefinition<Integer> MAX_THREADS = AttributeDefinition.builder("maxThreads", null, Integer.class).build();
   static final AttributeDefinition<Integer> CORE_THREADS = AttributeDefinition.builder("coreThreads", null, Integer.class).build();
   static final AttributeDefinition<Long> KEEP_ALIVE_TIME = AttributeDefinition.builder("keepAliveTime", null, Long.class).build();
   static final AttributeDefinition<Integer> QUEUE_LENGTH = AttributeDefinition.builder("queue-length", null, Integer.class).build();
   static final AttributeDefinition<Boolean> NON_BLOCKING = AttributeDefinition.builder("non-blocking", null, Boolean.class).build();

   private final AttributeSet attributes;
   private final Attribute<String> name;
   private final Attribute<String> threadFactory;
   private final Attribute<Integer> maxThreads;
   private final Attribute<Integer> coreThreads;
   private final Attribute<Long> keepAliveTime;
   private final Attribute<Integer> queueLength;
   private final Attribute<Boolean> nonBlocking;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(BoundedThreadPoolConfiguration.class, NAME, THREAD_FACTORY, MAX_THREADS, CORE_THREADS,
            KEEP_ALIVE_TIME, QUEUE_LENGTH, NON_BLOCKING);
   }

   BoundedThreadPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.threadFactory = attributes.attribute(THREAD_FACTORY);
      this.maxThreads = attributes.attribute(MAX_THREADS);
      this.coreThreads = attributes.attribute(CORE_THREADS);
      this.keepAliveTime = attributes.attribute(KEEP_ALIVE_TIME);
      this.queueLength = attributes.attribute(QUEUE_LENGTH);
      this.nonBlocking = attributes.attribute(NON_BLOCKING);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return name.get();
   }

   public String threadFactory() {
      return threadFactory.get();
   }

   public Integer getMaxThreads() {
      return maxThreads.get();
   }

   public Integer getCoreThreads() {
      return coreThreads.get();
   }

   public Long getKeepAliveTime() {
      return keepAliveTime.get();
   }

   public Integer getQueueLength() {
      return queueLength.get();
   }

   public Boolean isNonBlocking() {
      return nonBlocking.get();
   }

   @Override
   public String toString() {
      return "BoundedThreadPoolConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

}
