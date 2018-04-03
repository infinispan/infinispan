package org.infinispan.tools;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "tools-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
