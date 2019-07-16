package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.remoting.transport.Transport;

/*
 * @since 10.0
 */
public class JGroupsConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<Transport> TRANSPORT = AttributeDefinition
         .builder("transport", null, Transport.class).copier(IdentityAttributeCopier.INSTANCE).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JGroupsConfiguration.class, TRANSPORT);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.JGROUPS.getLocalName());

   private final Attribute<Transport> transport;
   private final List<StackFileConfiguration> stackFileConfigurations;
   private final List<StackConfiguration> stackConfigurations;
   private final AttributeSet attributes;
   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   JGroupsConfiguration(AttributeSet attributes, List<StackFileConfiguration> stackFileConfigurations, List<StackConfiguration> stackConfigurations) {
      this.attributes = attributes.checkProtection();
      this.transport = attributes.attribute(TRANSPORT);
      this.stackFileConfigurations = stackFileConfigurations;
      this.stackConfigurations = stackConfigurations;
      this.subElements.addAll(stackFileConfigurations);
      this.subElements.addAll(stackConfigurations);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public Transport transport() {
      return transport.get();
   }

   public List<StackFileConfiguration> stackFiles() {
      return stackFileConfigurations;
   }

   public List<StackConfiguration> stacks() {
      return stackConfigurations;
   }

   public boolean isClustered() {
      return transport.get() != null;
   }

   @Override
   public String toString() {
      return "JGroupsConfiguration{" +
            "transport=" + transport +
            ", stackFileConfigurations=" + stackFileConfigurations +
            ", stackConfigurations=" + stackConfigurations +
            '}';
   }

}
