package org.infinispan.server.test;

import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;

public class ContainerUtil {

   public static String getIpAddressFromContainer(GenericContainer container) {
      InspectContainerResponse containerInfo = container.getContainerInfo();
      ContainerNetwork network = containerInfo.getNetworkSettings().getNetworks().values().iterator().next();
      return network.getIpAddress();
   }
}
