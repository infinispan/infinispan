package org.infinispan.query;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "query-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
