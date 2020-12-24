package org.infinispan.server.test.core;

import org.testcontainers.containers.GenericContainer;

/**
 * Container utilities
 *
 * @author Dan Berindei
 * @since 12.0
 */
public class Containers {
   public static String ipAddress(GenericContainer<?> container) {
      return container.getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next().getIpAddress();
   }
}
