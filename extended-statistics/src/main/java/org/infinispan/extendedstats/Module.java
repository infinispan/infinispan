package org.infinispan.extendedstats;

import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "extended-statistics", requiredModules = "core")
public class Module implements ModuleLifecycle {
}
