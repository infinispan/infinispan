package org.infinispan.manager;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * Allow tests to create instances of {@link ModuleRepository}.
 */
public class TestModuleRepository {
   private static final ModuleRepository MODULE_REPOSITORY =
      new ModuleRepository.Builder(TestModuleRepository.class.getClassLoader()).build(new GlobalConfigurationBuilder().build());

   public static ModuleRepository newModuleRepository(ClassLoader classLoader, GlobalConfiguration globalConfiguration) {
      return new ModuleRepository.Builder(classLoader).build(globalConfiguration);
   }

   public static ModuleRepository defaultModuleRepository() {
      return MODULE_REPOSITORY;
   }
}
