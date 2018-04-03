package org.infinispan.server.hotrod;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "server-hotrod-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
