package org.infinispan.server.test.core.container;

import java.net.InetAddress;

import org.infinispan.commons.test.Exceptions;

public abstract class AbstractTestDriverContainer implements TestDriverContainer {

   @Override
   public InetAddress getServerAddress() {
      // We talk directly to the container, and not through forwarded addresses on localhost because of
      // https://github.com/testcontainers/testcontainers-java/issues/452
      return Exceptions.unchecked(() -> InetAddress.getByName(getIpAddress()));
   }
}
