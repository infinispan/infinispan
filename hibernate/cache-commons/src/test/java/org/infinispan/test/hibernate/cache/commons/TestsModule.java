package org.infinispan.test.hibernate.cache.commons;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "hibernate-commons-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
