package org.infinispan.server.test.core;

import static org.infinispan.commons.test.Eventually.eventually;
import static org.infinispan.server.Server.DEFAULT_SERVER_CONFIG;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_LOG_FILE;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.server.Server;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.Base58;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.Network;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerInfinispanServerDriver extends AbstractInfinispanServerDriver {
   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ContainerInfinispanServerDriver.class);
   private static final String STARTUP_MESSAGE_REGEX = ".*ISPN080001.*";
   private static final String SHUTDOWN_MESSAGE_REGEX = ".*ISPN080003.*";
   private static final String CLUSTER_VIEW_REGEX = ".*ISPN000094.*(?<=\\()(%d)(?=\\)).*";
   private static final int TIMEOUT_SECONDS = Integer.getInteger(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_TIMEOUT_SECONDS, 45);
   public static final String INFINISPAN_SERVER_HOME = "/opt/infinispan";
   public static final int JMX_PORT = 9999;
   public static final String JDK_BASE_IMAGE_NAME = "jboss/base-jdk:11";
   public static final String IMAGE_USER = "200";
   private final InfinispanGenericContainer[] containers;
   private final String[] volumes;
   private String name;
   ImageFromDockerfile image;

   protected ContainerInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(
            configuration,
            getDockerBridgeAddress()
      );
      this.containers = new InfinispanGenericContainer[configuration.numServers()];
      this.volumes = new String[configuration.numServers()];
   }

   static InetAddress getDockerBridgeAddress() {
      DockerClient dockerClient = DockerClientFactory.instance().client();
      Network bridge = dockerClient.inspectNetworkCmd().withNetworkId("bridge").exec();
      String gateway = bridge.getIpam().getConfig().get(0).getGateway();
      return Exceptions.unchecked(() -> InetAddress.getByName(gateway));
   }

   @Override
   protected void start(String name, File rootDir, File configurationFile) {
      this.name = name;
      // Build a skeleton server layout
      createServerHierarchy(rootDir);
      // Build the command-line that launches the server
      List<String> args = new ArrayList<>();
      args.add("bin/server.sh");
      args.add("-c");
      args.add(configurationFile.getName());
      args.add("-b");
      args.add("SITE_LOCAL");
      args.add("-Djgroups.bind.address=SITE_LOCAL");
      args.add("-j");
      args.add(System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CLUSTER_STACK, "test-tcp"));
      args.add("-Dinfinispan.cluster.name=" + name);
      args.add("-D" + TEST_HOST_ADDRESS + "=" + testHostAddress.getHostAddress());
      if (configuration.isJMXEnabled()) {
         args.add("-Dcom.sun.management.jmxremote.port=" + JMX_PORT);
         args.add("-Dcom.sun.management.jmxremote.authenticate=false");
         args.add("-Dcom.sun.management.jmxremote.ssl=false");
      }

      String logFile = System.getProperty(INFINISPAN_TEST_SERVER_LOG_FILE);
      if (logFile != null) {
         Path logPath = Paths.get(logFile);
         String logFileName = logPath.getFileName().toString();
         if (logPath.isAbsolute()) {
            try {
               // we need to copy the log file to the conf dir because the withFileFromPath("test"..) will overwrite
               // everything
               Files.copy(logPath, new File(getConfDir(), logFileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
               throw new IllegalStateException("Cannot copy the log file", e);
            }
         }
         args.add("-l");
         args.add(logFileName);
      }
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, Paths.get(INFINISPAN_SERVER_HOME, DEFAULT_SERVER_CONFIG).toString());
      properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, name);
      properties.setProperty(TEST_HOST_ADDRESS, testHostAddress.getHostName());
      configuration.properties().forEach((k, v) -> args.add("-D" + k + "=" + StringPropertyReplacer.replaceProperties((String) v, properties)));
      configureSite(args);
      boolean preserveImageAfterTest = Boolean.parseBoolean(configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_PRESERVE_IMAGE, "false"));
      Path tmp = Paths.get(CommonsTestingUtil.tmpDirectory(this.getClass()));

      File libDir = new File(rootDir, "lib");
      libDir.mkdirs();
      copyArtifactsToUserLibDir(libDir);

      image = new ImageFromDockerfile("testcontainers/" + Base58.randomString(16).toLowerCase(), !preserveImageAfterTest)
            .withFileFromPath("test", rootDir.toPath())
            .withFileFromPath("tmp", tmp)
            .withFileFromPath("lib", libDir.toPath());
      final boolean prebuiltImage;
      final String imageName;
      String baseImageName = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME);
      if (baseImageName == null) {
         String serverOutputDir = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
         if (serverOutputDir == null) {
            // We try to use the latest public image for this major.minor version
            imageName = "infinispan/server:" + Version.getMajorMinor();
            prebuiltImage = true;
            log.infof("Using prebuilt image '%s'", imageName);
         } else {
            // We build our local image based on the supplied server
            Path serverOutputPath = Paths.get(serverOutputDir).normalize();
            imageName = JDK_BASE_IMAGE_NAME;
            image
                  .withFileFromPath("target", serverOutputPath.getParent())
                  .withFileFromPath("src", serverOutputPath.getParent().getParent().resolve("src"))
                  .withFileFromPath("build", cleanServerDirectory(serverOutputPath));
            prebuiltImage = false;
            log.infof("Using local image from server built at '%s'", serverOutputPath);
         }
      } else {
         imageName = baseImageName;
         prebuiltImage = true;
         log.infof("Using prebuilt image '%s'", imageName);
      }
      image.withDockerfileFromBuilder(builder -> {
         builder
               .from(imageName)
               .env("INFINISPAN_SERVER_HOME", INFINISPAN_SERVER_HOME)
               .env("INFINISPAN_VERSION", Version.getVersion())
               .label("name", "Infinispan Server")
               .label("version", Version.getVersion())
               .label("release", Version.getVersion())
               .label("architecture", "x86_64");

         if (!prebuiltImage) {
            builder.copy("build", INFINISPAN_SERVER_HOME);
         }
         // Copy the resources to a location from where they can be added to the image
         try {
            URL resource = ContainerInfinispanServerDriver.class.getResource("/overlay");
            if (resource != null) {
               URI overlayUri = resource.toURI();
               if ("jar".equals(overlayUri.getScheme())) {
                  try (FileSystem fileSystem = FileSystems.newFileSystem(overlayUri, Collections.emptyMap())) {
                     Files.walkFileTree(fileSystem.getPath("/overlay"), new CommonsTestingUtil.CopyFileVisitor(tmp, true, f -> {
                        f.setExecutable(true, false);
                     }));
                  }
               } else {
                  Files.walkFileTree(Paths.get(overlayUri), new CommonsTestingUtil.CopyFileVisitor(tmp, true, f -> {
                     f.setExecutable(true, false);
                  }));
               }
            }
         } catch (Exception e) {
            throw new RuntimeException(e);
         }

         builder.copy("test", INFINISPAN_SERVER_HOME + "/server")
               .copy("tmp", INFINISPAN_SERVER_HOME)
               .workDir(INFINISPAN_SERVER_HOME)
               .entryPoint(args.toArray(new String[]{}))
               .expose(
                     11222, // Protocol endpoint
                     11221, // Memcached endpoint
                     7800,  // JGroups TCP
                     46655, // JGroups UDP
                     9999   // JMX Remoting
               );

         builder
               .copy("lib", serverPathFrom("lib"))
               .user("root")
               .run("chown", "-R", IMAGE_USER, INFINISPAN_SERVER_HOME)
               .run("chmod", "-R", "g+rw", INFINISPAN_SERVER_HOME)
               .user(IMAGE_USER);
      });

      int numServers = configuration.numServers();
      CountdownLatchLoggingConsumer clusterLatch = new CountdownLatchLoggingConsumer(numServers, String.format(CLUSTER_VIEW_REGEX, numServers));
      if (configuration.isParallelStartup()) {
         CountdownLatchLoggingConsumer startupLatch = new CountdownLatchLoggingConsumer(numServers, STARTUP_MESSAGE_REGEX);
         IntStream.range(0, configuration.numServers()).forEach(i -> createContainer(i, startupLatch, clusterLatch));
         Exceptions.unchecked(() -> startupLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      } else {
         for (int i = 0; i < configuration.numServers(); i++) {
            CountdownLatchLoggingConsumer startupLatch = new CountdownLatchLoggingConsumer(1, STARTUP_MESSAGE_REGEX);
            createContainer(i, startupLatch, clusterLatch);
            Exceptions.unchecked(() -> startupLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
         }
      }
      // Ensure that a cluster of numServers has actually formed before proceeding
      Exceptions.unchecked(() -> clusterLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
   }

   public InfinispanGenericContainer getContainer(int i) {
      if(containers.length <= i) {
         throw new IllegalStateException("Container " + i + " has not been initialized");
      }
      return containers[i];
   }

   private void configureSite(List<String> args) {
      if (configuration.site() == null) {
         return;
      }
      args.add("-Drelay.site_name=" + configuration.site());
      args.add("-Djgroups.cluster.mcast_port=" + configuration.siteDiscoveryPort());
   }

   /*
    * Removing the `server/data` and `server/log` directories from the local server directory to prevent issues with
    * the tests
    */
   private Path cleanServerDirectory(Path serverOutputPath) {
      Util.recursiveFileRemove(serverOutputPath.resolve("server").resolve("data").toString());
      Util.recursiveFileRemove(serverOutputPath.resolve("server").resolve("log").toString());
      return serverOutputPath;
   }

   private GenericContainer createContainer(int i, Consumer<OutputFrame>... logConsumers) {

      if (this.volumes[i] == null) {
         String volumeName = UUID.randomUUID().toString();
         DockerClientFactory.instance().client().createVolumeCmd().withName(volumeName).exec();
         this.volumes[i] = volumeName;
      }

      GenericContainer container = new GenericContainer<>(image)
         .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMounts(
            Arrays.asList(new Mount().withSource(this.volumes[i]).withTarget(serverPath()).withType(MountType.VOLUME))
         ));
      // Process any enhancers
      container.withLogConsumer(new JBossLoggingConsumer(LogFactory.getLogger(name)).withPrefix(Integer.toString(i)));
      for (Consumer<OutputFrame> consumer : logConsumers)
         container.withLogConsumer(consumer);

      log.infof("Starting container %d", i);
      container.start();
      containers[i] = new InfinispanGenericContainer(container);
      log.infof("Started container %d", i);
      return container;
   }

   @Override
   protected void stop() {
      for (int i = 0; i < containers.length; i++) {
         log.infof("Stopping container %d", i);
         stop(i);
         log.infof("Stopped container %d", i);
      }

      // See https://github.com/testcontainers/testcontainers-java/issues/2276
      ThreadLeakChecker.ignoreThreadsContaining("tc-okhttp-stream-");
   }

   @Override
   public boolean isRunning(int server) {
      return containers[server].isRunning();
   }

   @Override
   public InetSocketAddress getServerSocket(int server, int port) {
      return new InetSocketAddress(getServerAddress(server), port);
   }

   @Override
   public InetAddress getServerAddress(int server) {
      InfinispanGenericContainer container = containers[server];
      return container.getIpAddress();
   }

   @Override
   public void pause(int server) {
      InfinispanGenericContainer container = containers[server];
      container.pause();
      eventually("Container wasn't paused.", () -> container.isPaused());
      System.out.printf("[%d] PAUSE \n", server);
   }

   @Override
   public void resume(int server) {
      InfinispanGenericContainer container = containers[server];
      container.resume();
      eventually("Container didn't resume.", () -> isRunning(server));
      System.out.printf("[%d] RESUME \n", server);
   }

   @Override
   public void stop(int server) {
      InfinispanGenericContainer container = containers[server];
      // can fail during the startup
      if (container != null) {
         CountdownLatchLoggingConsumer latch = new CountdownLatchLoggingConsumer(1, SHUTDOWN_MESSAGE_REGEX);
         container.withLogConsumer(latch);
         container.stop();
         eventually("Container wasn't stopped.", () -> !isRunning(server));
         System.out.printf("[%d] STOP \n", server);
      }
   }

   @Override
   public void kill(int server) {
      InfinispanGenericContainer container = containers[server];
      // can fail during the startup
      if (container != null) {
         container.kill();
         eventually("Container wasn't killed.", () -> !isRunning(server));
         System.out.printf("[%d] KILL \n", server);
      }
   }

   @Override
   public void restart(int server) {
      if (isRunning(server)) {
         throw new IllegalStateException("Server " + server + " is still running");
      }
      CountdownLatchLoggingConsumer startupLatch = new CountdownLatchLoggingConsumer(1, STARTUP_MESSAGE_REGEX);
      // We can stop the server by doing a rest call. TestContainers has a state about each container. We clean that state
      stop(server);

      log.infof("Restarting container %d", server);
      createContainer(server, startupLatch);
      Exceptions.unchecked(() -> startupLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
   }

   @Override
   public void restartCluster() {
      for (int i = 0; i < configuration.numServers(); i++) {
         restart(i);
      }
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server) {
      return Exceptions.unchecked(() -> {
         InfinispanGenericContainer container = containers[server];
         ContainerNetwork network = container.getContainerNetwork();
         JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", network.getIpAddress(), JMX_PORT));
         JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
         return jmxConnector.getMBeanServerConnection();
      });
   }

   @Override
   public int getTimeout() {
      return TIMEOUT_SECONDS;
   }

   private String serverPath() {
      return String.format("%s/server", INFINISPAN_SERVER_HOME);
   }

   private String serverPathFrom(String path) {
      return String.format("%s/%s", serverPath(), path);
   }
}
