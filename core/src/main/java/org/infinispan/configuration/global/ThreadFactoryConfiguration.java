package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.factories.threads.DefaultNonBlockingThreadFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;

class ThreadFactoryConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<ThreadGroup> GROUP = AttributeDefinition.builder("groupName", null, ThreadGroup.class)
         .serializer(new AttributeSerializer<ThreadGroup, ConfigurationInfo, ConfigurationBuilderInfo>() {
            @Override
            public Object getSerializationValue(Attribute<ThreadGroup> attribute, ConfigurationInfo configurationElement) {
               return attribute.get().getName();
            }
         }).build();
   static final AttributeDefinition<String> THREAD_NAME_PATTERN = AttributeDefinition.builder("threadNamePattern", null, String.class).build();
   static final AttributeDefinition<Integer> PRIORITY = AttributeDefinition.builder("priority", null, Integer.class).build();

   private final AttributeSet attributes;
   private final Attribute<String> name;
   private final Attribute<ThreadGroup> group;
   private final Attribute<String> threadNamePattern;
   private final Attribute<Integer> priority;
   private String nodeName;

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.THREAD_FACTORY.getLocalName());

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ThreadFactoryConfiguration.class, NAME, GROUP, THREAD_NAME_PATTERN, PRIORITY);
   }

   ThreadFactoryConfiguration(AttributeSet attributes, String nodeName) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.group = attributes.attribute(GROUP);
      this.threadNamePattern = attributes.attribute(THREAD_NAME_PATTERN);
      this.priority = attributes.attribute(PRIORITY);
      this.nodeName = nodeName;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;

   }

   public DefaultThreadFactory getThreadFactory(boolean isNonBlocking) {
      if (isNonBlocking) {
         return new DefaultNonBlockingThreadFactory(name.get(), group.get(), priority.get(), threadNamePattern.get(), nodeName, null);
      }
      return new DefaultThreadFactory(name.get(), group.get(), priority.get(), threadNamePattern.get(), nodeName, null);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public Attribute<String> name() {
      return name;
   }

   public Attribute<ThreadGroup> groupName() {
      return group;
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
