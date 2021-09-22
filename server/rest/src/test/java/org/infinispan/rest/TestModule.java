package org.infinispan.rest;

import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * {@link InfinispanModule} annotation is required for component annotation processing
 */
@InfinispanModule(name = "server-rest-tests")
public class TestModule implements ModuleLifecycle {
}
