package org.infinispan.extendedstats;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "extended-statistics-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
