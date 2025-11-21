package org.infinispan.server.test.core;

import org.testcontainers.containers.GenericContainer;

/**
 * We can stop a container by doing rest calls. In this case, the TestContainers will have a wrong state.
 * Also, the TestContainers stop method is killing the container. See: https://github.com/testcontainers/testcontainers-java/issues/2608
 *
 * @deprecated 16.0
 * use {@link org.infinispan.testcontainers.InfinispanGenericContainer}
 */
@Deprecated(since = "16.0")
public class InfinispanGenericContainer extends org.infinispan.testcontainers.InfinispanGenericContainer {
   public InfinispanGenericContainer(GenericContainer genericContainer) {
      super(genericContainer);
   }
}
