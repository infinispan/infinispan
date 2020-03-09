package org.infinispan.server.test.core.container;

import org.infinispan.server.test.core.CountdownLatchLoggingConsumer;

import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ContainerNetwork;

public class TestDriverClientContainer extends AbstractTestDriverContainer {

   private Container delegate;

   public TestDriverClientContainer(Container delegate) {
      this.delegate = delegate;
   }

   @Override
   public void stop() {
      // it is not the code running in the server side responsible to stop the container
   }

   @Override
   public boolean isRunning() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void execInContainer(String command) {
      throw new UnsupportedOperationException();
   }

   @Override
   public String getIpAddress() {
      ContainerNetwork network = delegate.getNetworkSettings().getNetworks().values().iterator().next();
      return network.getIpAddress();
   }

   @Override
   public String getLogs() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void withLogConsumer(CountdownLatchLoggingConsumer latch) {
      throw new UnsupportedOperationException();
   }
}
