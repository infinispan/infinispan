package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.CachedThreadPoolConfiguration.NAME;
import static org.infinispan.configuration.global.CachedThreadPoolConfiguration.THREAD_FACTORY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;

/*
 * @since 10.0
 */
public class CachedThreadPoolConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<CachedThreadPoolConfiguration>, ThreadPoolBuilderAdapter {
   private final AttributeSet attributes;

   CachedThreadPoolConfigurationBuilder(GlobalConfigurationBuilder globalConfig, String name) {
      super(globalConfig);
      attributes = CachedThreadPoolConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public CachedThreadPoolConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   public CachedThreadPoolConfigurationBuilder threadFactory(String threadFactory) {
      attributes.attribute(THREAD_FACTORY).set(threadFactory);
      return this;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public CachedThreadPoolConfiguration create() {
      return new CachedThreadPoolConfiguration(attributes.protect());
   }

   @Override
   public CachedThreadPoolConfigurationBuilder read(CachedThreadPoolConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "CachedThreadPoolConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public ThreadPoolConfiguration asThreadPoolConfigurationBuilder() {
      ThreadPoolConfigurationBuilder builder = new ThreadPoolConfigurationBuilder(getGlobalConfig());
      builder.threadPoolFactory(CachedThreadPoolExecutorFactory.create());
      DefaultThreadFactory threadFactory = getGlobalConfig().threads().getThreadFactory(threadFactory()).create().getThreadFactory(false);
      builder.threadFactory(threadFactory);
      builder.name(name());
      return builder.create();
   }

   public String threadFactory() {
      return attributes.attribute(ScheduledThreadPoolConfiguration.THREAD_FACTORY).get();
   }
}
