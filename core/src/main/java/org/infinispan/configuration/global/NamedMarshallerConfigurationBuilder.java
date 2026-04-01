package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Builder for {@link NamedMarshallerConfiguration}.
 *
 * @author William Burns
 * @since 16.2
 */
public class NamedMarshallerConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<NamedMarshallerConfiguration> {

   private final AttributeSet attributes;

   NamedMarshallerConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.attributes = NamedMarshallerConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Sets the name for this marshaller.
    *
    * @param name the marshaller name
    * @return this builder for method chaining
    */
   public NamedMarshallerConfigurationBuilder name(String name) {
      if (name == null || name.isEmpty()) {
         throw new IllegalArgumentException("Marshaller name cannot be null or empty");
      }
      attributes.attribute(NamedMarshallerConfiguration.NAME).set(name);
      return this;
   }

   /**
    * Sets the marshaller class name to be instantiated.
    * This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    *
    * @param className the fully qualified class name of the marshaller
    * @return this builder for method chaining
    */
   public NamedMarshallerConfigurationBuilder marshallerClass(String className) {
      if (className == null || className.isEmpty()) {
         throw new IllegalArgumentException("Marshaller class name cannot be null or empty");
      }
      attributes.attribute(NamedMarshallerConfiguration.MARSHALLER_CLASS).set(className);
      attributes.attribute(NamedMarshallerConfiguration.MARSHALLER).set(null);
      return this;
   }

   /**
    * Sets the marshaller instance.
    * This method is mutually exclusive with {@link #marshallerClass(String)}.
    *
    * @param marshaller the marshaller instance
    * @return this builder for method chaining
    */
   public NamedMarshallerConfigurationBuilder marshaller(Marshaller marshaller) {
      if (marshaller == null) {
         throw new IllegalArgumentException("Marshaller cannot be null");
      }
      attributes.attribute(NamedMarshallerConfiguration.MARSHALLER).set(marshaller);
      attributes.attribute(NamedMarshallerConfiguration.MARSHALLER_CLASS).set(null);
      return this;
   }

   @Override
   public void validate() {
      String name = attributes.attribute(NamedMarshallerConfiguration.NAME).get();
      if (name == null || name.isEmpty()) {
         throw new IllegalStateException("Named marshaller must have a name");
      }

      String className = attributes.attribute(NamedMarshallerConfiguration.MARSHALLER_CLASS).get();
      Marshaller marshallerInstance = attributes.attribute(NamedMarshallerConfiguration.MARSHALLER).get();

      if (className == null && marshallerInstance == null) {
         throw new IllegalStateException("Named marshaller '" + name + "' must have either a marshaller class or instance");
      }

      if (className != null && marshallerInstance != null) {
         throw new IllegalStateException("Named marshaller '" + name + "' cannot have both a marshaller class and instance");
      }
   }

   @Override
   public NamedMarshallerConfiguration create() {
      return new NamedMarshallerConfiguration(attributes.protect());
   }

   @Override
   public NamedMarshallerConfigurationBuilder read(NamedMarshallerConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
