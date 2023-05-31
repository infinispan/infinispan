package org.infinispan.configuration.global;


import static org.infinispan.configuration.global.ScheduledThreadPoolConfiguration.NAME;
import static org.infinispan.configuration.global.ScheduledThreadPoolConfiguration.THREAD_FACTORY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;

/*
 * @since 10.0
 */
public class ScheduledThreadPoolConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ScheduledThreadPoolConfiguration>, ThreadPoolBuilderAdapter {
   private final AttributeSet attributes;

   ScheduledThreadPoolConfigurationBuilder(GlobalConfigurationBuilder globalConfig, String name) {
      super(globalConfig);
      attributes = ScheduledThreadPoolConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ScheduledThreadPoolConfigurationBuilder threadFactory(String threadFactory) {
      attributes.attribute(THREAD_FACTORY).set(threadFactory);
      return this;
   }

   public String threadFactory() {
      return attributes.attribute(THREAD_FACTORY).get();
   }

   public String name() {
      return attributes.attribute(CachedThreadPoolConfiguration.NAME).get();
   }

   @Override
   public ScheduledThreadPoolConfiguration create() {
      return new ScheduledThreadPoolConfiguration(attributes.protect());
   }

   @Override
   public ScheduledThreadPoolConfigurationBuilder read(ScheduledThreadPoolConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "ScheduledThreadPoolConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public ThreadPoolConfiguration asThreadPoolConfigurationBuilder() {
      ThreadPoolConfigurationBuilder builder = new ThreadPoolConfigurationBuilder(getGlobalConfig());
      builder.threadPoolFactory(ScheduledThreadPoolExecutorFactory.create());
      DefaultThreadFactory threadFactory = getGlobalConfig().threads().getThreadFactory(threadFactory()).create().getThreadFactory(false);
      builder.threadFactory(threadFactory);
      builder.name(name());
      return builder.create();
   }
}
