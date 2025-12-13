package org.infinispan.server.test.core;

import java.net.InetAddress;

import org.infinispan.commons.test.Exceptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Network;

/**
 * Container utilities
 *
 * @author Dan Berindei
 * @since 12.0
 */
public class Containers {

   public static final DockerClient DOCKER_CLIENT = DockerClientFactory.instance().client();
   public static String ipAddress(GenericContainer<?> container) {
      return container.getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next().getIpAddress();
   }

   public static InetAddress getContainerNetworkGateway() {
      return getContainerNetworkGateway("bridge");
   }

   public static InetAddress getContainerNetworkGateway(String id) {
      DockerClient dockerClient = DockerClientFactory.instance().client();
      Network bridge = dockerClient.inspectNetworkCmd().withNetworkId(id).exec();
      String gateway = bridge.getIpam().getConfig().get(0).getGateway();
      return Exceptions.unchecked(() -> InetAddress.getByName(gateway));
   }

   public static String imageArchitecture() {
      return switch (System.getProperty("os.arch")) {
         case "x86_64", "amd64" -> "amd64";
         case "aarch64" -> "arm64";
         default -> throw new IllegalArgumentException("Unknown architecture");
      };
   }
}
