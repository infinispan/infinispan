package org.infinispan.testcontainers;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * We can stop a container by doing rest calls. In this case, the TestContainers will have a wrong state.
 * Also, the TestContainers stop method is killing the container. See: https://github.com/testcontainers/testcontainers-java/issues/2608
 *
 * @since 16.0
 */
public class InfinispanGenericContainer {

   private final GenericContainer genericContainer;
   private final String containerId;
   private boolean isKilled = false;

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

   public void start() {
      if (!isRunning()) {
         dockerClient().startContainerCmd(this.containerId).exec();
      }
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

   public boolean isKilled() {
      return isKilled;
   }

   public void setKilled(boolean isKilled) {
      this.isKilled = isKilled;
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
      return unchecked(() -> InetAddress.getByName(getNetworkIpAddress()));
   }

   // Copied from org.infinispan.commons.test.Exceptions
   <T> T unchecked(Callable<T> callable) {
      try {
         return callable.call();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         throw (cause instanceof RuntimeException) ? (RuntimeException) cause : new RuntimeException(cause);
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public String getNetworkIpAddress() {
      return getNetworkIpAddresses().values().iterator().next();
   }

   public Map<String, String> getNetworkIpAddresses() {
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
      return containerInfo.getNetworkSettings().getNetworks().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getIpAddress()));
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

   private DockerClient dockerClient() {
      return DockerClientFactory.instance().client();
   }

   public GenericContainer getGenericContainer() {
      return genericContainer;
   }

   public void uploadCoverageInfoToHost(String containerPath, String destinationPath) {
      this.genericContainer.copyFileFromContainer(containerPath, destinationPath);
   }
}
