package org.infinispan.modules;

import java.util.Collection;

import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Module metadata.
 * <p>
 * <b>NOTE:</b> Not public API: Internal use only!
 *
 * @author Dan Berindei
 * @since 10.0
 */
public interface ModuleMetadataBuilder {

   String getModuleName();

   Collection<String> getRequiredDependencies();

   Collection<String> getOptionalDependencies();

   ModuleLifecycle newModuleLifecycle();

   void registerMetadata(ModuleLifecycle.ModuleBuilder builder);
}
