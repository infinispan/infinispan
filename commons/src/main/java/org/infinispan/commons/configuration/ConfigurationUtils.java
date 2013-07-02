package org.infinispan.commons.configuration;

import org.infinispan.commons.CacheConfigurationException;

/**
 * ConfigurationUtils. Contains utility methods used in configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public final class ConfigurationUtils {

   private ConfigurationUtils() {}

   @SuppressWarnings("unchecked")
   public static <B> Class<? extends Builder<B>> builderFor(B built) {
      BuiltBy builtBy = built.getClass().getAnnotation(BuiltBy.class);
      if (builtBy == null) {
         throw new CacheConfigurationException("Missing BuiltBy annotation for configuration bean " + built.getClass().getName());
      }
      return (Class<? extends Builder<B>>) builtBy.value();
   }
}
