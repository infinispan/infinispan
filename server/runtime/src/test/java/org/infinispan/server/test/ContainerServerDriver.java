package org.infinispan.server.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerServerDriver extends ServerDriver {
   private List<GenericContainer> containers;

   protected ContainerServerDriver(String name, ServerTestConfiguration configuration) {
      super(name, configuration);
   }

   @Override
   protected void before() {
      ImageFromDockerfile image = new ImageFromDockerfile()
            .withDockerfileFromBuilder(builder ->
                  builder
                        .from("jboss/base-jdk:11")
                        .build());
      containers = new ArrayList<>(configuration.numServers());
      for (int i = 0; i < configuration.numServers(); i++) {
         containers.add(new GenericContainer(image)
               .withExposedPorts(
                     7600,  // JGroups TCP
                     11222, // Hot Rod
                     8080,  // HTTP
                     11211  // Memcached
               ));
      }
   }

   @Override
   protected void after() {
      for (GenericContainer container : containers) {
         container.stop();
      }
      containers.clear();
   }

   @Override
   public InetSocketAddress getServerAddress(int server, int port) {
      return null;
   }
}
