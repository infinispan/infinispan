package org.infinispan.server.test.core;

import static org.infinispan.commons.test.Eventually.eventually;
import static org.infinispan.server.Server.DEFAULT_SERVER_CONFIG;
import static org.infinispan.server.test.core.Containers.DOCKER_CLIENT;
import static org.infinispan.server.test.core.Containers.getDockerBridgeAddress;
import static org.infinispan.server.test.core.Containers.imageArchitecture;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_ULIMIT;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_LOG_FILE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnector;
import javax.management.remote.rmi.RMIServer;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.test.Ansi;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.commons.util.OS;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.server.Server;
import org.infinispan.testcontainers.InfinispanGenericContainer;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.statement.RawStatement;

import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.Ulimit;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerInfinispanServerDriver extends AbstractInfinispanServerDriver {
   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ContainerInfinispanServerDriver.class);
   private static final String STARTUP_MESSAGE_REGEX = ".*ISPN080001.*";
   private static final String SHUTDOWN_MESSAGE_REGEX = ".*ISPN080003.*";
   private static final String CLUSTER_VIEW_REGEX = ".*ISPN000093.*(?<=\\()(%1$d)(?=\\)).*|.*ISPN000094.*(?<=\\()(%1$d)(?=\\)).*";
   private static final int TIMEOUT_SECONDS = Integer.getInteger(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_TIMEOUT_SECONDS, 45);
   private static final Long IMAGE_MEMORY = Long.getLong(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_MEMORY, null);
   private static final Long IMAGE_MEMORY_SWAP = Long.getLong(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_MEMORY_SWAP, null);
   public static final String INFINISPAN_SERVER_HOME = "/opt/infinispan";
   public static final String JACOCO_COVERAGE_CONTAINER_PATH = INFINISPAN_SERVER_HOME + "/bin/jacoco.exec";
   public static final String JACOCO_COVERAGE_HOST_PATH = "target/";
   public static final String JDK_BASE_IMAGE_NAME = "eclipse-temurin:25-ubi10-minimal";
   private static final String[] IMAGE_DEPENDENCIES = {
         "file",
         "gzip",
         "iproute",
         "lsof",
         "tar",
         "vim-minimal"
   };
   public static final String IMAGE_USER = "185";
   public static final Integer[] EXPOSED_PORTS = {
         11222, // Protocol endpoint
         11221, // Memcached endpoint
         11223, // Alternate Hot Rod endpoint
         11224, // Alternate REST endpoint
         11225, // Alternate single port endpoint
         7800,  // JGroups TCP
         46655, // JGroups UDP
         9999   // JMX Remoting
   };
   public static final String SNAPSHOT_IMAGE = "localhost/infinispan/server-snapshot";
   private final List<InfinispanGenericContainer> containers;
   private final String[] volumes;
   private String name;
   private String fullName;
   ImageFromDockerfile image;
   private static final List<String> sites = new ArrayList<>();
   private final NettyLeakDetectionLoggingConsumer leakDetectionLoggingConsumer = new NettyLeakDetectionLoggingConsumer();

   static {
      // Ensure there are no left-overs from previous runs
      cleanup();
   }

   protected ContainerInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(
            configuration,
            getDockerBridgeAddress()
      );
      int totalAmount = configuration.expectedServers() > 0 ? configuration.expectedServers() : configuration.numServers();
      this.containers = new ArrayList<>(totalAmount);
      // Fill the array list with nulls and then we directly call set
      for (int i = 0; i < totalAmount; ++i) {
         this.containers.add(null);
      }
      this.volumes = new String[totalAmount];
   }

   public static void cleanup() {
      cleanup(SNAPSHOT_IMAGE);
   }

   public static void cleanup(String imageName) {
      try {
         log.infof("Removing temporary image %s", imageName);
         DOCKER_CLIENT.removeImageCmd(imageName).exec();
         log.infof("Removed temporary image %s", imageName);
      } catch (Exception e) {
         // Ignore
      }
   }

   @Override
   protected void start(String fqcn, File rootDir) {
      configureImage(fqcn, rootDir);

      int numServers = configuration.numServers();
      CountdownLatchLoggingConsumer clusterLatch = new CountdownLatchLoggingConsumer(numServers, String.format(CLUSTER_VIEW_REGEX,
            configuration.expectedServers() > 0 ? configuration.expectedServers() : numServers));
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

   public void configureImage(String fqcn) {
      status = ComponentStatus.INITIALIZING;
      try {
         configureImage(fqcn, rootDir);
      } catch (Throwable t) {
         log.errorf(t, "Unable to configure server image for %s", name);
         status = ComponentStatus.FAILED;
         throw t;
      }
   }

   private void configureImage(String fqcn, File rootDir) {
      this.name = abbreviate(fqcn);
      this.fullName = fqcn;
      // If properties define the cluster stack let that take priority over the system property
      String jGroupsStack = !configuration.properties().containsKey(Server.INFINISPAN_CLUSTER_STACK) ?
            System.getProperty(Server.INFINISPAN_CLUSTER_STACK) : null;
      // Build a skeleton server layout
      createServerHierarchy(rootDir);
      // Build the command-line that launches the server
      List<String> args = new ArrayList<>();
      args.add("bin/server.sh");
      args.add("-c");
      args.add(new File(configuration.configurationFile()).getName());
      args.add("-b");
      args.add("SITE_LOCAL");
      args.add("-Djgroups.bind.address=SITE_LOCAL");
      if (jGroupsStack != null) {
         args.add("-j");
         args.add(jGroupsStack);
      }
      args.add("-Dinfinispan.cluster.name=" + (configuration.getClusterName() != null ? configuration.getClusterName() : name));
      args.add("-D" + TEST_HOST_ADDRESS + "=" + testHostAddress.getHostAddress());
      if (configuration.isJMXEnabled()) {
         args.add("--jmx");
         args.add(Integer.toString(JMX_PORT));
      }
      args.add("-Dio.netty.leakDetection.level=paranoid");

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
      copyArtifactsToDataDir();
      copyArtifactsToUserLibDir(libDir);
      if (isCoverage()) {
         addArtifactsToLibDir(libDir, "org.jacoco:org.jacoco.agent:" + System.getProperty("version.jacoco") + ":runtime");
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
      log.infof("Creating image %s", name);
      String versionToUse = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_VERSION, Version.getMajorMinor());
      final String imageName;
      String baseImageName = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME);
      if (baseImageName == null || baseImageName.isEmpty()) {
         String serverOutputDir = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
         if (serverOutputDir == null) {
            // We try to use the latest public image for this major.minor version
            imageName = "quay.io/infinispan/server:" + versionToUse;
            log.infof("Using prebuilt image '%s'", imageName);
         } else {
            imageName = createServerImage(serverOutputDir, versionToUse);
         }
      } else {
         imageName = baseImageName;
         log.infof("Using prebuilt image '%s'", imageName);
      }
      image = new ImageFromDockerfile("localhost/infinispan/server-" + name.toLowerCase(), !preserveImageAfterTest)
            .withFileFromPath("test", rootDir.toPath())
            .withFileFromPath("tmp", tmp)
            .withFileFromPath("lib", libDir.toPath())
            .withDockerfileFromBuilder(builder -> {
               builder
                     .from(imageName)
                     .env("INFINISPAN_SERVER_HOME", INFINISPAN_SERVER_HOME)
                     .env("INFINISPAN_VERSION", versionToUse)
                     .label("name", "Infinispan Server")
                     .label("version", versionToUse)
                     .label("release", versionToUse)
                     .label("architecture", imageArchitecture());

               builder
                     .user(IMAGE_USER)
                     .withStatement(new RawStatement("COPY", "--chown=" + IMAGE_USER + ":" + IMAGE_USER + " test " + INFINISPAN_SERVER_HOME + "/server"))
                     .withStatement(new RawStatement("COPY", "--chown=" + IMAGE_USER + ":" + IMAGE_USER + " tmp " + INFINISPAN_SERVER_HOME))
                     .withStatement(new RawStatement("COPY", "--chown=" + IMAGE_USER + ":" + IMAGE_USER + " lib " + serverPathFrom("lib")))
                     .workDir(INFINISPAN_SERVER_HOME)
                     .entryPoint(args.toArray(Util.EMPTY_STRING_ARRAY))
                     .expose(
                           EXPOSED_PORTS
                     )

               ;
            });
      image.get();
      log.infof("Created image %s", name);
   }

   /**
    * Starts an additional server outside that isn't part of {@link InfinispanServerTestConfiguration#numServers()}
    * number. This is useful to start servers at a later point.
    * <p>
    * This method can only be invoked after {@link #start(String)} has completed successfully
    *
    * @param expectedClusterSize The expected number of members in the cluster. Some implementations may not work if other nodes
    *                            outside of this driver are clustered and the argument could be ignored.
    * @param volumeName          The name of the shared volume to be used when a node restarts
    */
   public void startAdditionalServer(int expectedClusterSize, String volumeName) {
      try {
         CountdownLatchLoggingConsumer clusterLatch = new CountdownLatchLoggingConsumer(1, String.format(CLUSTER_VIEW_REGEX, expectedClusterSize));
         CountdownLatchLoggingConsumer startupLatch = new CountdownLatchLoggingConsumer(1, STARTUP_MESSAGE_REGEX);

         log.infof("Starting new single server for container %s with volume %s", name, volumeName);
         createContainer(createdContainers(), volumeName, startupLatch, clusterLatch);
         Exceptions.unchecked(() -> startupLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
         Exceptions.unchecked(() -> clusterLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
         status = ComponentStatus.RUNNING;
      } catch (Throwable t) {
         log.errorf(t, "Unable to start individual server for %s", name);
         status = ComponentStatus.FAILED;
         throw t;
      }
   }

   private int createdContainers() {
      return (int) containers.stream().filter(Objects::nonNull).count();
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
   private static Path cleanServerDirectory(Path serverOutputPath) {
      Util.recursiveFileRemove(serverOutputPath.resolve("server").resolve("data").toString());
      Util.recursiveFileRemove(serverOutputPath.resolve("server").resolve("log").toString());
      return serverOutputPath;
   }

   private static boolean isCoverage() {
      String jacocoVersion = System.getProperty("version.jacoco");
      return jacocoVersion != null;
   }

   private GenericContainer<?> createContainer(int i, Consumer<OutputFrame>... logConsumers) {
      return createContainer(i, null, logConsumers);
   }

   private GenericContainer<?> createContainer(int i, String volumeName, Consumer<OutputFrame>... logConsumers) {

      if (volumeName != null) {
         if (volumes[i] != null && !volumes[i].equals(volumeName)) {
            throw new IllegalArgumentException("Provided volume name " + volumeName + " doesn't match already present volume of " + volumes[i]);
         }
         volumes[i] = volumeName;
      } else if (Boolean.parseBoolean(configuration.properties().getProperty(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED))) {
         if (this.volumes[i] == null) {
            volumeName = Util.threadLocalRandomUUID().toString();
            DOCKER_CLIENT.createVolumeCmd().withName(volumeName).exec();
            this.volumes[i] = volumeName;
         } else {
            volumeName = volumes[i];
         }
      }

      // Only here so it is effectively immutable
      String volumeToUse = volumeName;

      GenericContainer<?> container = new GenericContainer<>(image)
            .withCreateContainerCmdModifier(cmd -> {
               if (volumeToUse != null) {
                  cmd.getHostConfig().withMounts(
                        Collections.singletonList(new Mount().withSource(volumeToUse).withTarget(serverPath() + "/data").withType(MountType.VOLUME))
                  );
               }
               if (IMAGE_MEMORY != null) {
                  cmd.getHostConfig().withMemory(IMAGE_MEMORY);
               }
               if (IMAGE_MEMORY_SWAP != null) {
                  cmd.getHostConfig().withMemorySwap(IMAGE_MEMORY_SWAP);
               }

               String ulimit = configuration.properties().getProperty(INFINISPAN_TEST_SERVER_CONTAINER_ULIMIT);
               if (ulimit != null) {
                  String[] softHard = ulimit.split(",");
                  assert softHard.length == 2 : "Ulimit property must have format '<soft>,<hard>'";
                  long soft = Long.parseLong(softHard[0]);
                  long hard = Long.parseLong(softHard[1]);
                  cmd.getHostConfig().withUlimits(new Ulimit[]{new Ulimit("nofile", soft, hard)});
               }
            });
      if (configuration.numServers() == 1 && (OS.getCurrentOs().equals(OS.MAC_OS) || OS.getCurrentOs().equals(OS.WINDOWS))) {
         container.addExposedPorts(
               11222, // HTTP/Hot Rod
               7800  // JGroups TCP
         );
      }
      String debug = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_DEBUG);
      StringBuilder javaOpts = new StringBuilder();
      String site = configuration.site();
      if (site != null) {
         if (!sites.contains(site)) {
            sites.add(site);
         }
      }
      if (i == 0 && site == null && !Boolean.parseBoolean(configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT))) {
         javaOpts.append("-D").append(JOIN_TIMEOUT).append("=0");
      }
      if (debug != null && Integer.parseInt(debug) == i) {
         javaOpts.append(" ").append(debugJvmOption());
         log.infof("Container debug enabled with options '%s'%n", javaOpts);
      }
      if (isCoverage()) {
         String jacocoVersion = System.getProperty("version.jacoco");
         javaOpts.append(" -javaagent:").append(INFINISPAN_SERVER_HOME).append("/server/lib/org.jacoco.agent-").append(jacocoVersion).append("-runtime.jar=output=file,destfile=").append(JACOCO_COVERAGE_CONTAINER_PATH).append(",append=true");
         // Code coverage requires more metaspace when the base image only has 96MB by default
         container.withEnv("JAVA_GC_MAX_METASPACE_SIZE", "512M");
      }

      if (!javaOpts.isEmpty()) {
         // We set both environment variables as an image from infinispan-images uses JAVA_OPTIONS
         // and an image from a directory would use JAVA_OPTS. And a custom image could be either.
         container.withEnv("JAVA_OPTS", javaOpts.toString());
         container.withEnv("JAVA_OPTIONS", javaOpts.toString());
      }

      // Process any enhancers
      final String color;
      final String reset;
      if (Ansi.useColor) {
         final int offset;
         if (site == null) {
            offset = 0;
         } else {
            offset = 4 * sites.indexOf(site);
         }
         color = Ansi.DISTINCT_COLORS[(offset + i) % Ansi.DISTINCT_COLORS.length];
         reset = Ansi.RESET;
      } else {
         color = "";
         reset = "";
      }
      String logPrefix = site == null ? name + "#" + i : name + "#" + site + "#" + i;
      container
            .withLogConsumer(new JBossLoggingConsumer(LogFactory.getLogger("CONTAINER")).withPrefix(color + "[" + logPrefix + "]").withSuffix(reset))
            .withLogConsumer(leakDetectionLoggingConsumer);
      for (Consumer<OutputFrame> consumer : logConsumers)
         container.withLogConsumer(consumer);

      String containerAndSite = i + (site != null ? "-" + site : "");
      log.infof("Starting container %s", containerAndSite);
      container.start();
      containers.set(i, new InfinispanGenericContainer(container));
      log.infof("Started container %s", containerAndSite);
      return container;
   }

   @Override
   public void stop() {
      String site = configuration.site();
      for (int i = 0; i < containers.size(); i++) {
         stop(i);
      }
      if (image != null) {
         cleanup(image.getDockerImageName());
      }
      // See https://github.com/testcontainers/testcontainers-java/issues/2276
      ThreadLeakChecker.ignoreThreadsContaining("docker-java-stream-");
      if (leakDetectionLoggingConsumer.leakDetected()) {
         throw new IllegalStateException("Leak detected");
      }
   }

   @Override
   public boolean isRunning(int server) {
      return containers.get(server).isRunning();
   }

   @Override
   public int serverCount() {
      return createdContainers();
   }

   @Override
   public InetSocketAddress getServerSocket(int server, int port) {
      return new InetSocketAddress(getServerAddress(server), port);
   }

   @Override
   public InetAddress getServerAddress(int server) {
      InfinispanGenericContainer container = containers.get(server);
      return container.getIpAddress();
   }

   @Override
   public void pause(int server) {
      InfinispanGenericContainer container = containers.get(server);
      container.pause();
      eventually("Container wasn't paused.", container::isPaused);
      System.out.printf("[%d] PAUSE %n", server);
   }

   @Override
   public void resume(int server) {
      InfinispanGenericContainer container = containers.get(server);
      container.resume();
      eventually("Container didn't resume.", () -> isRunning(server));
      System.out.printf("[%d] RESUME %n", server);
   }

   @Override
   public void stop(int server) {
      InfinispanGenericContainer container = containers.get(server);
      String site = configuration.site();
      String containerAndSite = server + (site != null ? "-" + site : "");
      // can fail during the startup
      if (container != null) {
         log.infof("Stopping container %s", containerAndSite);
         CountdownLatchLoggingConsumer latch = new CountdownLatchLoggingConsumer(1, SHUTDOWN_MESSAGE_REGEX);
         container.getGenericContainer().withLogConsumer(latch);
         container.stop();
         if (isCoverage() && !container.isKilled()) {
            //Getting Jacoco Coverage Report after stopping the container
            container.uploadCoverageInfoToHost(JACOCO_COVERAGE_CONTAINER_PATH, JACOCO_COVERAGE_HOST_PATH + this.fullName + "-" + server + ".exec");
         }
         eventually("Container wasn't stopped.", () -> !isRunning(server));
         log.infof("Stopped container %s", containerAndSite);
         System.out.printf("[%d] STOP %n", server);
      } else {
         log.infof("Container %s not present", containerAndSite);
      }
   }

   public String volumeId(int server) {
      return volumes[server];
   }

   @Override
   public void kill(int server) {
      InfinispanGenericContainer container = containers.get(server);
      // can fail during the startup
      if (container != null) {
         container.kill();
         eventually("Container wasn't killed.", () -> !isRunning(server));
         System.out.printf("[%d] KILL %n", server);
         container.setKilled(true);
      }
   }

   @Override
   public void restart(int server) {
      CountdownLatchLoggingConsumer startupLatch = new CountdownLatchLoggingConsumer(1, STARTUP_MESSAGE_REGEX);
      restart(server, startupLatch);
      Exceptions.unchecked(() -> startupLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
   }

   public void restart(int server, Consumer<OutputFrame> consumer) {
      if (isRunning(server)) {
         throw new IllegalStateException("Server " + server + " is still running");
      }
      // We can stop the server by doing a rest call. TestContainers has a state about each container. We clean that state
      stop(server);

      log.infof("Restarting container %d", server);
      createContainer(server, consumer);
   }

   @Override
   public void restartCluster() {
      for (int i = 0; i < configuration.numServers(); i++) {
         restart(i);
      }
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server, String username, String password, Consumer<Closeable> reaper) {
      return Exceptions.unchecked(() -> {
         InfinispanGenericContainer container = containers.get(server);
         final String urlPath = "/jndi/rmi://" + container.getIpAddress() + ":" + JMX_PORT + "/jmxrmi";
         JMXServiceURL url = new JMXServiceURL("rmi", "", 0, urlPath);
         Registry registry = LocateRegistry.getRegistry(container.getIpAddress().getHostAddress(), JMX_PORT);
         RMIServer stub = (RMIServer) registry.lookup("jmxrmi");
         JMXConnector connector = new RMIConnector(stub, null);
         Map<String, Object> env = new HashMap<>();
         env.put(JMXConnector.CREDENTIALS, new String[]{username, password});
         connector.connect(env);
         log.infof("Connecting to JMX URL %s", url.toString());
         reaper.accept(connector);
         return connector.getMBeanServerConnection();
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

   @Override
   public String syncFilesFromServer(int server, String path) {
      String serverPath = Paths.get(path).isAbsolute() ? path : INFINISPAN_SERVER_HOME + "/server/" + path;
      try (InputStream is = DOCKER_CLIENT.copyArchiveFromContainerCmd(containers.get(server).getContainerId(), serverPath).exec()) {
         TarArchiveInputStream tar = new TarArchiveInputStream(is);
         Path basePath = getRootDir().toPath().resolve(Integer.toString(server));
         Util.recursiveFileRemove(basePath.resolve(path));
         for (TarArchiveEntry entry = tar.getNextTarEntry(); entry != null; entry = tar.getNextTarEntry()) {
            Path entryPath = basePath.resolve(entry.getName());
            if (entry.isDirectory()) {
               Files.createDirectories(entryPath);
            } else {
               // Ensure the directory is there
               Files.createDirectories(entryPath.getParent());
               OutputStream os = Files.newOutputStream(entryPath);
               for (int b = tar.read(); b >= 0; b = tar.read()) {
                  os.write(b);
               }
               Util.close(os);
            }
         }
         return basePath.toString();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public String syncFilesToServer(int server, String path) {
      Path local = Paths.get(path);
      Path parent = local.getParent();
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           TarArchiveOutputStream tar = new TarArchiveOutputStream(bos)) {
         Files.walkFileTree(local, new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
               Path relativize = parent.relativize(dir);
               TarArchiveEntry entry = new TarArchiveEntry(dir.toFile(), relativize.toString());
               entry.setMode(040777);
               tar.putArchiveEntry(entry);
               tar.closeArchiveEntry();
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
               Path relativize = parent.relativize(file);
               TarArchiveEntry entry = new TarArchiveEntry(file.toFile(), relativize.toString());
               entry.setMode(0100666);
               tar.putArchiveEntry(entry);
               try (InputStream is = Files.newInputStream(file)) {
                  for (int b = is.read(); b >= 0; b = is.read()) {
                     tar.write(b);
                  }
               }
               tar.closeArchiveEntry();
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
               return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
               return FileVisitResult.CONTINUE;
            }
         });
         tar.close();
         DOCKER_CLIENT.copyArchiveToContainerCmd(containers.get(server).getContainerId())
               .withTarInputStream(new ByteArrayInputStream(bos.toByteArray()))
               .withRemotePath("/tmp").exec();
         return Paths.get("/tmp").resolve(local.getFileName()).toString();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   String createServerImage(String serverOutputDir, String versionToUse) {
      String snapshotImageName = configuration.properties().getProperty(
            TestSystemPropertyNames.INFINISPAN_TEST_SERVER_SNAPSHOT_IMAGE_NAME, SNAPSHOT_IMAGE);
      try {
         InspectImageResponse response = DOCKER_CLIENT.inspectImageCmd(snapshotImageName).exec();
         log.infof("Reusing existing image: %s", response);
         String name = response.getConfig().getImage();
         if (name == null || name.isEmpty()) {
            return snapshotImageName;
         }
         return name;
      } catch (NotFoundException e) {
         // We build our local image based on the supplied server directory
         Path serverOutputPath = Paths.get(serverOutputDir).normalize();
         if (Files.notExists(serverOutputPath)) {
            throw new RuntimeException("Cannot create server image: no server at " + serverOutputPath);
         }
         ImageFromDockerfile image = new ImageFromDockerfile(snapshotImageName, false)
               .withFileFromPath("build", cleanServerDirectory(serverOutputPath))
               .withDockerfileFromBuilder(builder -> builder
                     .from(JDK_BASE_IMAGE_NAME)
                     .env("INFINISPAN_SERVER_HOME", INFINISPAN_SERVER_HOME)
                     .env("INFINISPAN_VERSION", versionToUse)
                     .label("name", "Infinispan Server")
                     .label("version", versionToUse)
                     .label("release", versionToUse)
                     .label("architecture", imageArchitecture())
                     .withStatement(new RawStatement("COPY", "--chown=" + IMAGE_USER + ":" + IMAGE_USER + " build " + INFINISPAN_SERVER_HOME))
                     .user("root")
                     .run(String.format("microdnf install -y %s", String.join(" ", IMAGE_DEPENDENCIES)))
                     .user(IMAGE_USER));
         log.infof("Building server snapshot image %s from %s", snapshotImageName, serverOutputPath);
         image.get();
         return image.getDockerImageName();
      }
   }
}
