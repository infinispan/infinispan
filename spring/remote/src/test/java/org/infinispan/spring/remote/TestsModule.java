package org.infinispan.spring.remote;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "spring6-remote-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
