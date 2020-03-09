package org.infinispan.server.test.core;

import java.util.List;

import org.infinispan.server.test.core.container.TestDriverClientContainer;
import org.testcontainers.DockerClientFactory;

import com.github.dockerjava.api.model.Container;

/*
 * The code will be execute in the server side: See server/integration/README.md
 * In the server side, we lost the reference from the docker because it will be a new classpath.
 * We need to contact the DockerClientFactory.instance() to get all running containers
 * Once we get it: We delegate to TestDriverClientContainer
 */
public class RemoteContainerInfinispanServerDriver extends ContainerInfinispanServerDriver {
   public RemoteContainerInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration);
      List<Container> containersFromClient = DockerClientFactory.instance().client().listContainersCmd().withShowAll(false).exec();
      for (Container c : containersFromClient) {
         if (!c.getImage().startsWith("quay.io")) {
            this.containers.add(new TestDriverClientContainer(c));
         }
      }
   }
}
