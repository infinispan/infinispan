package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.BoundedThreadPoolConfiguration.CORE_THREADS;
import static org.infinispan.configuration.global.BoundedThreadPoolConfiguration.KEEP_ALIVE_TIME;
import static org.infinispan.configuration.global.BoundedThreadPoolConfiguration.MAX_THREADS;
import static org.infinispan.configuration.global.BoundedThreadPoolConfiguration.NON_BLOCKING;
import static org.infinispan.configuration.global.BoundedThreadPoolConfiguration.QUEUE_LENGTH;
import static org.infinispan.configuration.global.CachedThreadPoolConfiguration.NAME;
import static org.infinispan.configuration.global.CachedThreadPoolConfiguration.THREAD_FACTORY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.factories.threads.CoreExecutorFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;

/*
 * @since 10.0
 */
public class BoundedThreadPoolConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<BoundedThreadPoolConfiguration>, ThreadPoolBuilderAdapter {
   private final AttributeSet attributes;

   BoundedThreadPoolConfigurationBuilder(GlobalConfigurationBuilder globalConfig, String name) {
      super(globalConfig);
      attributes = BoundedThreadPoolConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public BoundedThreadPoolConfigurationBuilder threadFactory(String threadFactory) {
      attributes.attribute(THREAD_FACTORY).set(threadFactory);
      return this;
   }

   public BoundedThreadPoolConfigurationBuilder maxThreads(Integer maxThreads) {
      attributes.attribute(MAX_THREADS).set(maxThreads);
      return this;
   }

   public Integer maxThreads() {
      return attributes.attribute(MAX_THREADS).get();
   }

   public BoundedThreadPoolConfigurationBuilder coreThreads(Integer coreThreads) {
      attributes.attribute(CORE_THREADS).set(coreThreads);
      return this;
   }

   public Integer coreThreads() {
      return attributes.attribute(CORE_THREADS).get();
   }

   public BoundedThreadPoolConfigurationBuilder keepAliveTime(Long keepAlive) {
      attributes.attribute(KEEP_ALIVE_TIME).set(keepAlive);
      return this;
   }

   public Long keepAliveTime() {
      return attributes.attribute(KEEP_ALIVE_TIME).get();
   }

   public BoundedThreadPoolConfigurationBuilder queueLength(Integer queueLength) {
      attributes.attribute(QUEUE_LENGTH).set(queueLength);
      return this;
   }

   public Integer queueLength() {
      return attributes.attribute(QUEUE_LENGTH).get();
   }

   public BoundedThreadPoolConfigurationBuilder nonBlocking(Boolean nonBlocking) {
      attributes.attribute(NON_BLOCKING).set(nonBlocking);
      return this;
   }

   public Boolean isNonBlocking() {
      return attributes.attribute(NON_BLOCKING).get();
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public BoundedThreadPoolConfiguration create() {
      return new BoundedThreadPoolConfiguration(attributes.protect());
   }

   @Override
   public BoundedThreadPoolConfigurationBuilder read(BoundedThreadPoolConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   public String threadFactory() {
      return attributes.attribute(THREAD_FACTORY).get();
   }

   @Override
   public String toString() {
      return "BoundedThreadPoolConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public ThreadPoolConfiguration asThreadPoolConfigurationBuilder() {
      ThreadPoolConfigurationBuilder builder = new ThreadPoolConfigurationBuilder(getGlobalConfig());
      boolean isNonBlocking = isNonBlocking();
      builder.threadPoolFactory(CoreExecutorFactory.executorFactory(maxThreads(), coreThreads(), queueLength(),
            keepAliveTime(), isNonBlocking));
      builder.name(name());
      if (threadFactory() != null) {
         DefaultThreadFactory threadFactory = getGlobalConfig().threads().getThreadFactory(threadFactory()).create().getThreadFactory(isNonBlocking);
         builder.threadFactory(threadFactory);
      }
      return builder.create();
   }
}
