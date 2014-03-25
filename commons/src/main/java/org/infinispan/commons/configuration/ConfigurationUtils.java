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

   /**
    * Returns the builder that was used to build this class.  This is determined by the instance having a class that
    * has a {@link org.infinispan.commons.configuration.BuiltBy} annotation present on it.  If one is not present
    * a {@link org.infinispan.commons.CacheConfigurationException} is thrown
    * @param built The instance to find the builder for
    * @param <B> The type of builder
    * @return The builder for this instance
    * @throws CacheConfigurationException thrown if the instance class can't provide the builder
    */
   @SuppressWarnings("unchecked")
   public static <B> Class<? extends Builder<B>> builderFor(B built) throws CacheConfigurationException {
      BuiltBy builtBy = built.getClass().getAnnotation(BuiltBy.class);
      if (builtBy == null) {
         throw new CacheConfigurationException("Missing BuiltBy annotation for configuration bean " + built.getClass().getName());
      }
      return (Class<? extends Builder<B>>) builtBy.value();
   }

   /**
    * The same as {@link org.infinispan.commons.configuration.ConfigurationUtils#builderFor(Object)} except that it won't
    * throw an exception if no builder class is found.  Instead null will be returned.
    * @param built The instance to find the builder for
    * @param <B> The type of builder
    * @return The builder for this instance or null if there isn't one
    */
   @SuppressWarnings("unchecked")
   public static <B> Class<? extends Builder<B>> builderForNonStrict(B built) {
      BuiltBy builtBy = built.getClass().getAnnotation(BuiltBy.class);
      if (builtBy == null) {
         return null;
      }
      return (Class<? extends Builder<B>>) builtBy.value();
   }
}
