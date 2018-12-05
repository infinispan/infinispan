package org.infinispan.server.commons.features;


import java.lang.invoke.MethodHandles;
import java.util.Objects;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Features;

public class Feature {
   /**
    * Verify that the specified feature is enabled. Initializes features if the provided one is null, allowing for
    * lazily initialized Features instance.
    * @param features existing instance or null.
    * @param feature the feature string to check.
    * @param classLoader to create the {@link Features} instance with if it doesn't already exist.
    * @return the features instance that was checked, always non null
    * @throws org.infinispan.commons.CacheConfigurationException thrown if the feature was disabled
    */
   public static Features isAvailableOrThrowException(Features features, String feature, ClassLoader classLoader) {
      if (features == null) {
         features = new Features(classLoader);
      }
      isAvailableOrThrowException(features, feature);
      return features;
   }

   /**
    * If the specified feature is not available throw a {@link org.infinispan.commons.CacheConfigurationException}.
    * @param features existing instance.
    * @param feature the feature string to check.
    * @throws NullPointerException if either parameter is null.
    * @throws org.infinispan.commons.CacheConfigurationException thrown if the feature was disabled
    */
   public static void isAvailableOrThrowException(Features features, String feature) {
      Objects.requireNonNull(features, feature);
      if (!features.isAvailable(feature)) {
         throw LogFactory.getLog(MethodHandles.lookup().lookupClass()).featureDisabled(feature);
      }
   }

   /**
    * Create a {@link Features} instance using the provided {@link ClassLoader} and return true if the specified feature
    * is enabled.
    * @param feature the feature string to check.
    * @param classLoader the {@link ClassLoader} used to create the {@link Features} instance.
    * @return true if the specified feature is enabled
    */
   public static boolean isAvailable(String feature, ClassLoader classLoader) {
      return new Features(classLoader).isAvailable(feature);
   }
}
