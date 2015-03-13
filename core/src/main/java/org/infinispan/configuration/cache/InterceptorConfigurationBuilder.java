package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.configuration.cache.InterceptorConfiguration.*;

/**
 * This builder defines details of a specific custom interceptor.
 */
public class InterceptorConfigurationBuilder extends AbstractCustomInterceptorsConfigurationChildBuilder implements Builder<InterceptorConfiguration> {
   private static final Log log = LogFactory.getLog(InterceptorConfigurationBuilder.class);
   private final AttributeSet attributes;

   InterceptorConfigurationBuilder(CustomInterceptorsConfigurationBuilder builder) {
      super(builder);
      attributes = InterceptorConfiguration.attributeSet();
   }

   /**
    * Dictates that the custom interceptor appears immediately <i>after</i> the specified interceptor. If the specified
    * interceptor is not found in the interceptor chain, a {@link CacheConfigurationException} will be thrown when the
    * cache starts.
    *
    * @param after the class of the interceptor to look for
    */
   public InterceptorConfigurationBuilder after(Class<? extends CommandInterceptor> after) {
      attributes.attribute(AFTER).set(after);
      return this;
   }

   /**
    * Dictates that the custom interceptor appears immediately <i>before</i> the specified interceptor. If the specified
    * interceptor is not found in the interceptor chain, a {@link CacheConfigurationException} will be thrown when the
    * cache starts.
    *
    * @param before the class of the interceptor to look for
    */
   public InterceptorConfigurationBuilder before(Class<? extends CommandInterceptor> before) {
      attributes.attribute(BEFORE).set(before);
      return this;
   }

   /**
    * An instance of the new custom interceptor to add to the configuration.
    * @param interceptor an instance of {@link CommandInterceptor}
    */
   public InterceptorConfigurationBuilder interceptor(CommandInterceptor interceptor) {
      attributes.attribute(INTERCEPTOR).set(interceptor);
      return this;
   }

   /**
    * Specifies a position in the interceptor chain to place the new interceptor.  The index starts at 0 and goes up to
    * the number of interceptors in a given configuration.  An {@link IllegalArgumentException} is thrown if the index is
    * less than 0 or greater than the maximum number of interceptors in the chain.
    *
    * @param i positional index in the interceptor chain to place the new interceptor.
    */
   public InterceptorConfigurationBuilder index(int i) {
      if (i < 0) throw new IllegalArgumentException("Index cannot be negative");
      attributes.attribute(INDEX).set(i);
      return this;
   }

   /**
    * Specifies a position, denoted by the {@link Position} enumeration, where to place the new interceptor.
    *
    * @param p position to place the new interceptor
    */
   public InterceptorConfigurationBuilder position(Position p) {
      attributes.attribute(POSITION).set(p);
      return this;
   }

   /**
    * Sets interceptor properties
    *
    * @return this InterceptorConfigurationBuilder
    */
   public InterceptorConfigurationBuilder withProperties(Properties properties) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   /**
    * Clears the interceptor properties
    *
    * @return this InterceptorConfigurationBuilder
    */
   public InterceptorConfigurationBuilder clearProperties() {
      TypedProperties properties = attributes.attribute(PROPERTIES).asObject(TypedProperties.class);
      properties.clear();
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   public InterceptorConfigurationBuilder addProperty(String key, String value) {
      TypedProperties properties = attributes.attribute(PROPERTIES).asObject(TypedProperties.class);
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   public InterceptorConfigurationBuilder removeProperty(String key) {
      TypedProperties properties = attributes.attribute(PROPERTIES).asObject(TypedProperties.class);
      properties.remove(key);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   @Override
   public void validate() {
      // Make sure more than one 'position' isn't picked.
      int positions = 0;

      if (!attributes.attribute(BEFORE).isNull()) positions++;
      if (!attributes.attribute(AFTER).isNull()) positions++;
      if (attributes.attribute(INDEX).asInteger() > -1) positions++;
      if (attributes.attribute(POSITION).isModified()) positions++;

      CommandInterceptor interceptor = attributes.attribute(INTERCEPTOR).asObject(CommandInterceptor.class);
      switch (positions) {
         case 0:
            throw log.missingCustomInterceptorPosition(interceptor.getClass().getName());
         case 1:
            break;
         default:
            throw log.multipleCustomInterceptorPositions(interceptor.getClass().getName());
      }

      if (interceptor == null) {
         throw log.customInterceptorMissingClass();
      }
      else if (!(interceptor instanceof BaseCustomInterceptor)) {
         final String className = interceptor.getClass().getName();
         //Suppress noisy warnings if the interceptor is one of our own (like one of those from Query):
         if (! className.startsWith("org.infinispan.")) {
            log.suggestCustomInterceptorInheritance(className);
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public InterceptorConfiguration create() {
      return new InterceptorConfiguration(attributes.protect());
   }

   @Override
   public InterceptorConfigurationBuilder read(InterceptorConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "InterceptorConfigurationBuilder [attributes=" + attributes + "]";
   }
}
