package org.infinispan.server.test.core.persistence;

import static org.infinispan.server.test.core.Containers.DOCKER_CLIENT;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.Containers;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerDatabase extends Database {
   private static final Log log = LogFactory.getLog(ContainerDatabase.class);
   public static final String DB_PREFIX = "database.container.";
   private static final String ENV_PREFIX = DB_PREFIX + "env.";
   private final int port;
   private final String volumeName;
   private final boolean volumeRequired;
   private volatile JdbcContainerAdapter<?> container;

   public ContainerDatabase(String type, Properties properties) {
      super(type, properties);
      this.port = Integer.parseInt(dbProp(properties, "port"));
      this.volumeName = Util.threadLocalRandomUUID().toString();
      this.volumeRequired = Boolean.parseBoolean(dbProp(properties, "volume"));
      this.container = createContainer(true);
   }

   private JdbcContainerAdapter<?> createContainer(boolean createVolume) {
      Map<String, String> env = properties.entrySet().stream().filter(e -> e.getKey().toString().startsWith(ENV_PREFIX))
            .collect(Collectors.toMap(e -> e.getKey().toString().substring(ENV_PREFIX.length()), e -> e.getValue().toString()));
      ImageFromDockerfile image = new ImageFromDockerfile()
            .withDockerfileFromBuilder(builder -> {
               builder
                     .from(dbProp(properties, "name") + ":" + dbProp(properties, "tag"))
                     .expose(port)
                     .env(env)
                     .build();
            });
      this.container = new JdbcContainerAdapter<>(image, this)
            .withExposedPorts(port)
            .withPrivilegedMode(true)
            .waitingFor(Wait.forListeningPort());

      var initSql = initSqlFile();
      if (createVolume && initSql != null)
         container.withInitScript(initSql);

      String logMessageWaitStrategy = properties.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_LOG_MESSAGE);
      if (logMessageWaitStrategy != null) {
         container.waitingFor(new LogMessageWaitStrategy()
               .withRegEx(logMessageWaitStrategy)
               .withStartupTimeout(Duration.of(10, ChronoUnit.MINUTES)));
      }

      if (volumeRequired) {
         if (createVolume) {
            log.infof("Creating volume '%s'", volumeName);
            DOCKER_CLIENT.createVolumeCmd().withName(volumeName).exec();
            log.infof("Created volume '%s'", volumeName);
         }
         var volumeMount = dbProp(properties, "volumeMount");
         container.withCreateContainerCmdModifier(cmd ->
               cmd.getHostConfig().withMounts(
                     List.of(new Mount().withSource(volumeName).withTarget(volumeMount).withType(MountType.VOLUME))
               )
         );
      }
      return container;
   }

   private String dbProp(Properties props, String prop) {
      return props.getProperty(DB_PREFIX + prop);
   }

   @Override
   public void start() {
      log.infof("Starting database %s", getType());
      container.start();
   }

   @Override
   public void stop() {
      stop(true);
   }

   public void stop(boolean deleteVolume) {
      log.infof("Stopping database %s", getType());
      container.stop();
      log.infof("Stopped database %s", getType());
      if (volumeRequired && deleteVolume) {
         log.infof("Removing volume '%s'", volumeName);
         try {
            dockerClient().removeVolumeCmd(volumeName).exec();
         } catch (NotFoundException e) {
            log.infof("Volume '%s' not found", volumeName);
         }
         log.infof("Removed volume '%s'", volumeName);
      }
   }

   public void restart() {
      if (container.isRunning()) stop(false);
      container = createContainer(false);
      container.start();
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

   private DockerClient dockerClient() {
      return DockerClientFactory.instance().client();
   }

   static class JdbcContainerAdapter<SELF extends JdbcContainerAdapter<SELF>> extends JdbcDatabaseContainer<SELF> {

      final Database database;

      JdbcContainerAdapter(ImageFromDockerfile image, Database database) {
         super(image);
         this.database = database;
      }

      @Override
      public String getDriverClassName() {
         return database.driverClassName();
      }

      @Override
      protected void waitUntilContainerStarted() {
         // Override JdbcContainerAdapter#waitUntulContainerStarted to ensure that any additional waitStrategies defined,
         // e.g. LogMessageWaitStrategy, are respected
         getWaitStrategy().waitUntilReady(this);
         super.waitUntilContainerStarted();
      }

      @Override
      public String getJdbcUrl() {
         return database.jdbcUrl();
      }

      @Override
      public String getUsername() {
         return database.username();
      }

      @Override
      public String getPassword() {
         return database.password();
      }

      @Override
      protected String getTestQueryString() {
         return database.testQuery();
      }
   }
}
