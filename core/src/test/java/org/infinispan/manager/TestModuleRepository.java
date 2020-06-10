package org.infinispan.manager;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * Provide a default {@link ModuleRepository} for tests.
 */
public final class TestModuleRepository {

   private static final ModuleRepository MODULE_REPOSITORY = ModuleRepository.newModuleRepository(
         TestModuleRepository.class.getClassLoader(), new GlobalConfigurationBuilder().build());

   public static ModuleRepository defaultModuleRepository() {
      return MODULE_REPOSITORY;
   }
}
