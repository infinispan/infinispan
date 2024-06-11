package org.infinispan.client.hotrod;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "client-hotrod-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
