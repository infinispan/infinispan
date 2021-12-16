package org.infinispan.server.test.core.persistence;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.Containers;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerDatabase extends Database {
   private final static Log log = LogFactory.getLog(ContainerDatabase.class);
   private final static String ENV_PREFIX = "database.container.env.";
   private final GenericContainer container;
   private final int port;

   ContainerDatabase(String type, Properties properties) {
      super(type, properties);
      Map<String, String> env = properties.entrySet().stream().filter(e -> e.getKey().toString().startsWith(ENV_PREFIX))
            .collect(Collectors.toMap(e -> e.getKey().toString().substring(ENV_PREFIX.length()), e -> e.getValue().toString()));
      port = Integer.parseInt(properties.getProperty("database.container.port"));
      ImageFromDockerfile image = new ImageFromDockerfile()
            .withDockerfileFromBuilder(builder -> {
               builder
                     .from(properties.getProperty("database.container.name") + ":" + properties.getProperty("database.container.tag"))
                     .expose(port)
                     .env(env)
                     .build();
            });
      container = new GenericContainer(image)
            .withExposedPorts(port)
            .withPrivilegedMode(true)
            .waitingFor(Wait.forListeningPort());

      String logMessageWaitStrategy = properties.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_LOG_MESSAGE);
      if (logMessageWaitStrategy != null) {
         container.waitingFor(new LogMessageWaitStrategy()
               .withRegEx(logMessageWaitStrategy)
               .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES)));
      }
   }

   @Override
   public void start() {
      log.infof("Starting database %s", getType());
      container.start();
   }

   @Override
   public void stop() {
      log.infof("Stopping database %s", getType());
      container.stop();
      log.infof("Stopped database %s", getType());
      ContainerInfinispanServerDriver.removeDockerImage(container.getDockerImageName());
   }

   public int getPort() {
      return container.getMappedPort(port);
   }

   @Override
   public String jdbcUrl() {
      String address = Containers.ipAddress(container);
      Properties props = new Properties();
      props.setProperty("container.address", address);
      return StringPropertyReplacer.replaceProperties(super.jdbcUrl(), props);
   }

   @Override
   public String username() {
      Properties props = new Properties();
      return StringPropertyReplacer.replaceProperties(super.username(), props);
   }

   @Override
   public String password() {
      Properties props = new Properties();
      return StringPropertyReplacer.replaceProperties(super.password(), props);
   }
}
