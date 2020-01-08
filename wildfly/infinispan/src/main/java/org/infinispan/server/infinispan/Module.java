package org.infinispan.server.infinispan;

import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "server-infinispan")
public class Module implements ModuleLifecycle {
}
