package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Configuration for a named marshaller.
 *
 * @author William Burns
 * @since 16.2
 */
public class NamedMarshallerConfiguration extends ConfigurationElement<NamedMarshallerConfiguration> {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.NAME, null, String.class).immutable().build();
   static final AttributeDefinition<String> MARSHALLER_CLASS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MARSHALLER, null, String.class).immutable().build();
   static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller-instance", null, Marshaller.class).immutable().autoPersist(false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(NamedMarshallerConfiguration.class, NAME, MARSHALLER_CLASS, MARSHALLER);
   }

   private final Attribute<String> name;
   private final Attribute<String> marshallerClass;
   private final Attribute<Marshaller> marshaller;

   NamedMarshallerConfiguration(AttributeSet attributes) {
      super("named-marshaller", attributes);
      name = attributes.attribute(NAME);
      marshallerClass = attributes.attribute(MARSHALLER_CLASS);
      marshaller = attributes.attribute(MARSHALLER);
   }

   /**
    * Returns the name of this marshaller.
    *
    * @return the marshaller name
    */
   public String name() {
      return name.get();
   }

   /**
    * Returns the class name of the marshaller to instantiate, or {@code null} if a marshaller instance was provided.
    *
    * @return the marshaller class name
    */
   public String marshallerClass() {
      return marshallerClass.get();
   }

   /**
    * Returns the marshaller instance, or {@code null} if a class name was provided instead.
    *
    * @return the marshaller instance
    */
   public Marshaller marshaller() {
      return marshaller.get();
   }
}
