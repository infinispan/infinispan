package org.infinispan.factories.impl;

import java.util.Collection;
import java.util.List;

import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Module metadata. This interface is not intended to be implemented by handwritten code. Implementations are generated
 * via annotation processing of InfinispanModule annotation and friends.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public interface ModuleMetadataBuilder {

   interface ModuleBuilder {

      void registerComponentAccessor(String componentClassName, List<String> factoryComponentNames, ComponentAccessor<?> accessor);

      void registerMBeanMetadata(String componentClassName, MBeanMetadata mBeanMetadata);

      String getFactoryName(String componentName);
   }

   String getModuleName();

   Collection<String> getRequiredDependencies();

   Collection<String> getOptionalDependencies();

   ModuleLifecycle newModuleLifecycle();

   void registerMetadata(ModuleBuilder builder);
}
