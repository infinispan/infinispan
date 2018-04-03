package org.infinispan.stats;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "extended-statistics")
public class Module implements org.infinispan.lifecycle.ModuleLifecycle {
}
