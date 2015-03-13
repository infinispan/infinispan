package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.CacheConfigurationException;



public final class AttributeDefinition<T> implements Cloneable {
   private final String name;
   private final T defaultValue;
   private boolean immutable;
   private AttributeInitializer<T> initializer;
   private final AttributeValidator<T> validator;
   private final Class<?> type;

   AttributeDefinition(String name, T initialValue, Class<T> type, boolean immutable, AttributeValidator<T> validator, AttributeInitializer<T> initializer) {
      this.name = name;
      this.defaultValue = initialValue;
      this.immutable = immutable;
      this.initializer = initializer;
      this.validator = validator;
      this.type = type;
   }

   public String name() {
      return name;
   }

   @SuppressWarnings("unchecked")
   public Class<T> getType() {
      return (Class<T>) type;
   }

   public T getDefaultValue() {
      return initializer != null ? initializer().initialize() : defaultValue;
   }

   public boolean isImmutable() {
      return immutable;
   }

   public AttributeInitializer<T> initializer() {
      return initializer;
   }

   public void setInitializer(AttributeInitializer<T> initializer) {
      this.initializer = initializer;
   }

   AttributeValidator<T> validator() {
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
         return new Builder(name, defaultValue, defaultValue.getClass());
      } else {
         throw new CacheConfigurationException("Must specify type when passing null for AttributeDefinition " + name);
      }

   }

   public static <T> Builder<T> builder(String name, T initialValue, Class<T> klass) {
      return new Builder<>(name, initialValue, klass);
   }

   public static final class Builder<T> {
      private final String name;
      private boolean immutable = false;
      private final T defaultValue;
      private AttributeInitializer<T> initializer;
      private AttributeValidator<T> validator;
      private Class<?> type;

      private Builder(String name, T defaultValue, Class<T> type) {
         this.name = name;
         this.defaultValue = defaultValue;
         this.type = type;
      }

      public Builder<T> immutable() {
         this.immutable = true;
         return this;
      }

      public Builder<T> initializer(AttributeInitializer<T> initializer) {
         this.initializer = initializer;
         return this;
      }

      public Builder<T> validator(AttributeValidator<T> validator) {
         this.validator = validator;
         return this;
      }

      public AttributeDefinition<T> build() {
         return new AttributeDefinition(name, defaultValue, type, immutable, validator, initializer);
      }
   }

}
