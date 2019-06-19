package org.infinispan.server.test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerInfinispanServerDriver extends InfinispanServerDriver {
   private List<GenericContainer> containers;

   protected ContainerInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration);
   }

   @Override
   protected void start(String name, File rootDir, String configurationFile) {
      // Build a skeleton server layout
      createServerHierarchy(rootDir, null);

      ImageFromDockerfile image = new ImageFromDockerfile()
            .withDockerfileFromBuilder(builder ->
                  builder
                        .from("jboss/base-jdk:11")
                        .build())
            .withFileFromFile("/opt/infinispan-server", rootDir);
      containers = new ArrayList<>(configuration.numServers());
      for (int i = 0; i < configuration.numServers(); i++) {
         // Create directories which we will bind the container to
         File serverRoot = createServerHierarchy(rootDir, Integer.toString(i));
         GenericContainer container = new GenericContainer(image);
         container
               .withExposedPorts(
                     7600,  // JGroups TCP
                     11222  // Protocol endpoint
               )
               .withFileSystemBind("", "");
         containers.add(container);
         container.start();
      }
   }

   @Override
   protected void stop() {
      for (GenericContainer container : containers) {
         container.stop();
      }
      containers.clear();
   }

   @Override
   public InetSocketAddress getServerAddress(int server, int port) {
      GenericContainer container = containers.get(server);
      return InetSocketAddress.createUnresolved(container.getContainerIpAddress(), container.getMappedPort(port));
   }
}
