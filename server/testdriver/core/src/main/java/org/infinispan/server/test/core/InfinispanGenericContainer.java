package org.infinispan.server.test.core;

import java.net.InetAddress;

import org.infinispan.commons.test.Exceptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * We can stop a container by doing rest calls. In this case, the TestContainers will have a wrong state.
 * Also, the TestContainers stop method is killing the container. See: https://github.com/testcontainers/testcontainers-java/issues/2608
 */
public class InfinispanGenericContainer {

   private GenericContainer genericContainer;
   private String containerId;

   public InfinispanGenericContainer(GenericContainer genericContainer) {
      this.containerId = genericContainer.getContainerId();
      this.genericContainer = genericContainer;
   }

   public String getContainerId() {
      return containerId;
   }

   public void pause() {
      dockerClient().pauseContainerCmd(this.containerId).exec();
   }

   public void resume() {
      dockerClient().unpauseContainerCmd(this.containerId).exec();
   }

   public void stop() {
      // it could be stopped by the rest call and then NotModifiedException will be throw
      if (isRunning()) {
         dockerClient().stopContainerCmd(this.containerId).exec();
      }
   }

   public void kill() {
      dockerClient().killContainerCmd(this.containerId).exec();
      genericContainer.stop(); // we need clean the TestContainers resources
   }

   public boolean isRunning() {
      InspectContainerResponse containerInfo = containerInfo();
      return containerInfo != null ? containerInfo.getState().getRunning() : false;
   }

   public boolean isPaused() {
      InspectContainerResponse containerInfo = containerInfo();
      return containerInfo != null ? containerInfo.getState().getPaused() : false;
   }

   public ContainerNetwork getContainerNetwork() {
      InspectContainerResponse containerInfo = containerInfo();
      if (containerInfo == null) {
         throw new NullPointerException(String.format("The requested container %s have an invalid state", this.containerId));
      }
      return containerInfo.getNetworkSettings().getNetworks().values().iterator().next();
   }

   public InetAddress getIpAddress() {
      // We talk directly to the container, and not through forwarded addresses on localhost because of
      // https://github.com/testcontainers/testcontainers-java/issues/452
      return Exceptions.unchecked(() -> InetAddress.getByName(getNetworkIpAddress()));
   }

   public String getNetworkIpAddress() {
      InspectContainerResponse containerInfo = containerInfo();
      if (containerInfo == null) {
         throw new NullPointerException(String.format("The requested container %s have an invalid state", this.containerId));
      }
      /*
       * when the container is not running it will return an empty string
       * an empty string will be translated to localhost/127.0.0.1:11222
       * everything will fail because this is a test that is running in a container
       * failing fast with the error message can help you to not waste time
       */
      if (!containerInfo.getState().getRunning()) {
         throw new IllegalStateException("Server must be running");
      }
      ContainerNetwork network = containerInfo.getNetworkSettings().getNetworks().values().iterator().next();
      return network.getIpAddress();
   }

   public InspectContainerResponse containerInfo() {
      InspectContainerResponse containerInfo;
      try {
         containerInfo = dockerClient().inspectContainerCmd(this.containerId).exec();
      } catch (NotFoundException e) { // the container could be removed or not available
         containerInfo = null;
      }
      return containerInfo;
   }

   public String getLogs() {
      return this.genericContainer.getLogs();
   }

   public int getMappedPort(int port) {
      return this.genericContainer.getMappedPort(port);
   }

   public void withLogConsumer(CountdownLatchLoggingConsumer latch) {
      genericContainer.withLogConsumer(latch);
   }

   private DockerClient dockerClient() {
      return DockerClientFactory.instance().client();
   }
}
