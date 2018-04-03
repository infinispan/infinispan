package org.infinispan.modules;

import java.util.Collection;

import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.ModuleRepository;

/**
 * Module metadata
 *
 * @since 10.0
 * @author Dan Berindei
 */
public interface ModuleMetadataBuilder {
   String getModuleName();

   Collection<String> getRequiredDependencies();

   Collection<String> getOptionalDependencies();

   ModuleLifecycle newModuleLifecycle();

   void registerMetadata(ModuleRepository.Builder builder);
}
