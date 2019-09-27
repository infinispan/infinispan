package org.infinispan.factories.impl;

import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Modules implementing {@link org.infinispan.lifecycle.ModuleLifecycle} might need additional control over the created
 * components. They can hook into the module metadata building process during startup by implementing this interface
 * which allows them to register additional component metadata dynamically.
 * <p>
 * This interface is currently experimental, it does not constitute a public API and is only used for internal testing
 * purposes.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public interface DynamicModuleMetadataProvider {

   /**
    * Use the provided builder to add additional component metadata. Use the GlobalConfiguration to access configuration,
    * global or per-module (see GlobalConfiguration.module)
    *
    * @param moduleBuilder
    * @param globalConfiguration
    */
   void registerDynamicMetadata(ModuleMetadataBuilder.ModuleBuilder moduleBuilder, GlobalConfiguration globalConfiguration);
}
