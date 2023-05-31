package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.ThreadFactoryConfiguration.GROUP;
import static org.infinispan.configuration.global.ThreadFactoryConfiguration.NAME;
import static org.infinispan.configuration.global.ThreadFactoryConfiguration.PRIORITY;
import static org.infinispan.configuration.global.ThreadFactoryConfiguration.THREAD_NAME_PATTERN;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/*
 * @since 10.0
 */
public class ThreadFactoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ThreadFactoryConfiguration> {
   private final AttributeSet attributes;
   private String nodeName;

   ThreadFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig, String name) {
      super(globalConfig);
      attributes = ThreadFactoryConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ThreadFactoryConfigurationBuilder groupName(String threadGroupName) {
      attributes.attribute(GROUP).set(threadGroupName);
      return this;
   }

   public ThreadFactoryConfigurationBuilder threadNamePattern(String threadNamePattern) {
      attributes.attribute(THREAD_NAME_PATTERN).set(threadNamePattern);
      return this;
   }

   public ThreadFactoryConfigurationBuilder priority(Integer priority) {
      attributes.attribute(PRIORITY).set(priority);
      return this;
   }

   public ThreadFactoryConfigurationBuilder nodeName(String nodeName) {
      this.nodeName = nodeName;
      return this;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public Integer priority() {
      return attributes.attribute(PRIORITY).get();
   }

   public String groupName() {
      return attributes.attribute(GROUP).get();
   }

   public String threadNamePattern() {
      return attributes.attribute(THREAD_NAME_PATTERN).get();
   }

   public String nodeName() {
      return nodeName;
   }

   @Override
   public ThreadFactoryConfiguration create() {
      return new ThreadFactoryConfiguration(attributes.protect(), nodeName);
   }

   @Override
   public ThreadFactoryConfigurationBuilder read(ThreadFactoryConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
