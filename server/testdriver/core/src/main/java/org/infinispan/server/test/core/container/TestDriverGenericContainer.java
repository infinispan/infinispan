package org.infinispan.server.test.core.container;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.test.core.CountdownLatchLoggingConsumer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.model.ContainerNetwork;

public class TestDriverGenericContainer extends AbstractTestDriverContainer {

   private GenericContainer delegate;

   public TestDriverGenericContainer(GenericContainer delegate) {
      this.delegate = delegate;
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public boolean isRunning() {
      return delegate.isRunning();
   }

   @Override
   public void execInContainer(String command) {
      Container.ExecResult result = Exceptions.unchecked(() -> delegate.execInContainer(command));
      System.out.printf("[%s] %s %s\n", delegate.getContainerId(), command, result);
   }

   @Override
   public String getIpAddress() {
      ContainerNetwork network = delegate.getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next();
      return network.getIpAddress();
   }

   @Override
   public String getLogs() {
      return delegate.getLogs();
   }

   @Override
   public void withLogConsumer(CountdownLatchLoggingConsumer latch) {
      delegate.withLogConsumer(latch);
   }


}
