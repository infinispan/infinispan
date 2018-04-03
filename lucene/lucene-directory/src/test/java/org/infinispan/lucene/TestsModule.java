package org.infinispan.lucene;

import org.infinispan.factories.annotations.InfinispanModule;

/**
 * {@code InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "lucene-directory-tests")
public class TestsModule implements org.infinispan.lifecycle.ModuleLifecycle {
}
