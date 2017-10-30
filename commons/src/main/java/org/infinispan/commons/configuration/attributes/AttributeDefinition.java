package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.CacheConfigurationException;
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
   private final boolean local;
   private final AttributeCopier copier;
   private final AttributeInitializer<? extends T> initializer;
   private final AttributeValidator<? super T> validator;
   private final Class<T> type;

   AttributeDefinition(String name, String xmlName, T initialValue, Class<T> type,
                       boolean immutable, boolean autoPersist, boolean local,
                       AttributeCopier copier, AttributeValidator<? super T> validator,
                       AttributeInitializer<? extends T> initializer) {
      this.name = name;
      this.xmlName = xmlName;
      this.defaultValue = initialValue;
      this.immutable = immutable;
      this.autoPersist = autoPersist;
      this.local = local;
      this.copier = copier;
      this.initializer = initializer;
      this.validator = validator;
      this.type = type;
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

   public T getDefaultValue() {
      return initializer != null ? initializer().initialize() : defaultValue;
   }

   public boolean isImmutable() {
      return immutable;
   }

   public boolean isAutoPersist() {
      return autoPersist;
   }

   public boolean isLocal() {
      return local;
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

   public static final class Builder<T> {
      private final String name;
      private final T defaultValue;
      private final Class<T> type;

      private boolean immutable = false;
      private boolean autoPersist = true;
      private boolean local = false;
      private String xmlName;
      private AttributeCopier copier = null;
      private AttributeInitializer<? extends T> initializer;
      private AttributeValidator<? super T> validator;


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

      public Builder<T> autoPersist(boolean autoPersist) {
         this.autoPersist = autoPersist;
         return this;
      }

      public Builder<T> local(boolean local) {
         this.local = local;
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
         return new AttributeDefinition<T>(name, xmlName == null ? Util.xmlify(name) : xmlName, defaultValue, type, immutable, autoPersist, local, copier, validator, initializer);
      }
   }

}
