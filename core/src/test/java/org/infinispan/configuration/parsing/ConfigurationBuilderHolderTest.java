package org.infinispan.configuration.parsing;

import static org.infinispan.configuration.cache.StorageType.HEAP;
import static org.infinispan.eviction.EvictionStrategy.REMOVE;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.parsing.ConfigurationBuilderHolderTest")
public class ConfigurationBuilderHolderTest {
   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*needs either off-heap or a binary compatible.*")
   public void testConfigNotValid() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();

      ConfigurationBuilder concreteConfigBuilder = holder.newConfigurationBuilder("concrete");
      // This isn't valid as we need a binary encoding
      concreteConfigBuilder.memory().maxSize("1.5 GB").storage(HEAP).whenFull(REMOVE);

      holder.resolveConfigurations();
   }

   public void testTemplateHasRequiredConfigForValidation() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      ConfigurationBuilder templateConfigBuilder = holder.newConfigurationBuilder("template");

      // The concreteConfig below is not valid without the media type being set on the template
      templateConfigBuilder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM);
      templateConfigBuilder.template(true);

      ConfigurationBuilder concreteConfigBuilder = holder.newConfigurationBuilder("concrete");
      concreteConfigBuilder.configuration("template");
      concreteConfigBuilder.memory().maxSize("1.5 GB").storage(HEAP).whenFull(REMOVE);

      holder.resolveConfigurations();
   }
}
