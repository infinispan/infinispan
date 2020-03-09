package org.infinispan.server.test.core.container;

import java.net.InetAddress;

import org.infinispan.server.test.core.CountdownLatchLoggingConsumer;

public interface TestDriverContainer {

   void stop();

   boolean isRunning();

   InetAddress getServerAddress();

   void execInContainer(String command);

   String getIpAddress();

   String getLogs();

   void withLogConsumer(CountdownLatchLoggingConsumer latch);
}
