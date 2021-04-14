package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.factories.threads.BlockingThreadFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.factories.threads.NonBlockingThreadFactory;

class ThreadFactoryConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> GROUP = AttributeDefinition.builder("groupName", null, String.class).build();
   static final AttributeDefinition<String> THREAD_NAME_PATTERN = AttributeDefinition.builder("threadNamePattern", null, String.class).build();
   static final AttributeDefinition<Integer> PRIORITY = AttributeDefinition.builder("priority", null, Integer.class).build();

   private final AttributeSet attributes;
   private final Attribute<String> name;
   private final Attribute<String> groupName;
   private final Attribute<String> threadNamePattern;
   private final Attribute<Integer> priority;
   private String nodeName;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ThreadFactoryConfiguration.class, NAME, GROUP, THREAD_NAME_PATTERN, PRIORITY);
   }

   ThreadFactoryConfiguration(AttributeSet attributes, String nodeName) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.groupName = attributes.attribute(GROUP);
      this.threadNamePattern = attributes.attribute(THREAD_NAME_PATTERN);
      this.priority = attributes.attribute(PRIORITY);
      this.nodeName = nodeName;
   }

   public DefaultThreadFactory getThreadFactory(boolean isNonBlocking) {
      if (isNonBlocking) {
         return new NonBlockingThreadFactory(name.get(), groupName.get(), priority.get(), threadNamePattern.get(), nodeName, null);
      }
      return new BlockingThreadFactory(name.get(), groupName.get(), priority.get(), threadNamePattern.get(), nodeName, null);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public Attribute<String> name() {
      return name;
   }

   public Attribute<String> groupName() {
      return groupName;
   }

   public Attribute<String> threadPattern() {
      return threadNamePattern;
   }

   public Attribute<Integer> priority() {
      return priority;
   }

   public String getNodeName() {
      return nodeName;
   }

   @Override
   public String toString() {
      return "ThreadFactoryConfiguration{" +
            "attributes=" + attributes +
            ", nodeName='" + nodeName + '\'' +
            '}';
   }

}
