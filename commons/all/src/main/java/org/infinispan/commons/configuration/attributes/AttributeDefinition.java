package org.infinispan.commons.configuration.attributes;

import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.util.Util;

/**
 *
 * AttributeDefinition. Defines the characteristics of a configuration attribute. It is used to
 * construct an actual {@link Attribute} holder.
 *
 * An attribute definition has the following characteristics:
 * <ul>
 * <li>A name</li>
 * <li>A default value or a value initializer</li>
 * <li>A type, which needs to be specified if it cannot be inferred from the default value, i.e.
 * when it is null</li>
 * <li>Whether an attribute is immutable or not, i.e. whether its value is constant after
 * initialization or it can be changed</li>
 * <li>A validator which intercepts invalid values</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public final class AttributeDefinition<T> {
   private final String name;
   private final String xmlName;
   private final T defaultValue;
   private final boolean immutable;
   private final boolean autoPersist;
   private final boolean global;
   private final AttributeCopier copier;
   private final AttributeInitializer<? extends T> initializer;
   private final AttributeValidator<? super T> validator;
   private final AttributeSerializer<? super T, ? extends ConfigurationInfo, ? extends ConfigurationBuilderInfo> serializerConfig;
   private final Class<T> type;

   AttributeDefinition(String name, String xmlName, T initialValue, Class<T> type,
                       boolean immutable, boolean autoPersist, boolean global,
                       AttributeCopier copier, AttributeValidator<? super T> validator,
                       AttributeInitializer<? extends T> initializer,
                       AttributeSerializer<? super T, ? extends ConfigurationInfo, ? extends ConfigurationBuilderInfo> serializerConfig) {
      this.name = name;
      this.xmlName = xmlName;
      this.defaultValue = initialValue;
      this.immutable = immutable;
      this.autoPersist = autoPersist;
      this.global = global;
      this.copier = copier;
      this.initializer = initializer;
      this.validator = validator;
      this.type = type;
      this.serializerConfig = serializerConfig;

   }

   public String name() {
      return name;
   }

   public String xmlName() {
      return xmlName;
   }

   public Class<T> getType() {
      return type;
   }

   public AttributeSerializer<? super T, ? extends ConfigurationInfo, ? extends ConfigurationBuilderInfo> getSerializerConfig() {
      return serializerConfig;
   }

   public T getDefaultValue() {
      return initializer != null ? initializer().initialize() : defaultValue;
   }

   public boolean isImmutable() {
      return immutable;
   }

   public boolean isAutoPersist() {
      return autoPersist;
   }

   public boolean isGlobal() {
      return global;
   }

   public AttributeCopier copier() {
      return copier;
   }

   public AttributeInitializer<? extends T> initializer() {
      return initializer;
   }

   AttributeValidator<? super T> validator() {
      return validator;
   }

   public Attribute<T> toAttribute() {
      return new Attribute<T>(this);
   }

   public void validate(T value) {
      if (validator != null) {
         validator.validate(value);
      }
   }

   public static <T> Builder<T> builder(String name, T defaultValue) {
      if (defaultValue != null) {
         return new Builder<T>(name, defaultValue, (Class<T>) defaultValue.getClass());
      } else {
         throw new CacheConfigurationException("Must specify type when passing null for AttributeDefinition " + name);
      }
   }

   public static <T> Builder<T> builder(String name, T defaultValue, Class<T> klass) {
      return new Builder<>(name, defaultValue, klass);
   }

   public static <T> Builder<Class<? extends T>> classBuilder(String name, Class<T> klass) {
      return new Builder(name, null, Class.class);
   }

   public static <T> Builder<Supplier<? extends T>> supplierBuilder(String name, Class<T> klass) {
      return new Builder(name, null, Supplier.class);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AttributeDefinition<?> that = (AttributeDefinition<?>) o;

      if (immutable != that.immutable) return false;
      if (autoPersist != that.autoPersist) return false;
      if (global != that.global) return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;
      if (xmlName != null ? !xmlName.equals(that.xmlName) : that.xmlName != null) return false;
      if (defaultValue != null ? !defaultValue.equals(that.defaultValue) : that.defaultValue != null) return false;
      if (copier != null ? !copier.equals(that.copier) : that.copier != null) return false;
      if (initializer != null ? !initializer.equals(that.initializer) : that.initializer != null) return false;
      if (validator != null ? !validator.equals(that.validator) : that.validator != null) return false;
      return type != null ? type.equals(that.type) : that.type == null;
   }

   @Override
   public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (xmlName != null ? xmlName.hashCode() : 0);
      result = 31 * result + (defaultValue != null ? defaultValue.hashCode() : 0);
      result = 31 * result + (immutable ? 1 : 0);
      result = 31 * result + (autoPersist ? 1 : 0);
      result = 31 * result + (global ? 1 : 0);
      result = 31 * result + (copier != null ? copier.hashCode() : 0);
      result = 31 * result + (initializer != null ? initializer.hashCode() : 0);
      result = 31 * result + (validator != null ? validator.hashCode() : 0);
      result = 31 * result + (type != null ? type.hashCode() : 0);
      return result;
   }

   public static final class Builder<T> {
      private final String name;
      private final T defaultValue;
      private final Class<T> type;

      private boolean immutable = false;
      private boolean autoPersist = true;
      private boolean global = true;
      private String xmlName;
      private AttributeCopier copier = null;
      private AttributeInitializer<? extends T> initializer;
      private AttributeValidator<? super T> validator;
      private AttributeSerializer<? super T, ? extends ConfigurationInfo, ? extends ConfigurationBuilderInfo> serializer;


      private Builder(String name, T defaultValue, Class<T> type) {
         this.name = name;
         this.defaultValue = defaultValue;
         this.type = type;
      }

      public Builder<T> immutable() {
         this.immutable = true;
         return this;
      }

      public Builder<T> copier(AttributeCopier copier) {
         this.copier = copier;
         return this;
      }

      public Builder<T> initializer(AttributeInitializer<? extends T> initializer) {
         this.initializer = initializer;
         return this;
      }

      public Builder<T> serializer(AttributeSerializer<? super T, ? extends ConfigurationInfo, ? extends ConfigurationBuilderInfo> serializer) {
         this.serializer = serializer;
         return this;
      }

      public Builder<T> autoPersist(boolean autoPersist) {
         this.autoPersist = autoPersist;
         return this;
      }

      public Builder<T> global(boolean global) {
         this.global = global;
         return this;
      }

      public Builder<T> validator(AttributeValidator<? super T> validator) {
         this.validator = validator;
         return this;
      }

      public Builder<T> xmlName(String xmlName) {
         this.xmlName = xmlName;
         return this;
      }

      public AttributeDefinition<T> build() {
         return new AttributeDefinition<>(name, xmlName == null ? Util.xmlify(name) : xmlName, defaultValue, type, immutable, autoPersist, global, copier, validator, initializer, serializer == null ? new DefaultSerializer<>(xmlName == null ? Util.xmlify(name) : xmlName) : serializer);
      }
   }

}
