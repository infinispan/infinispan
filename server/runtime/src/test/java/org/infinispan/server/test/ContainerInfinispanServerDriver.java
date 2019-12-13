package org.infinispan.server.test;

import static org.infinispan.server.test.ContainerUtil.getIpAddressFromContainer;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.Version;
import org.infinispan.server.Server;
import org.infinispan.test.Exceptions;
import org.infinispan.util.logging.LogFactory;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolvedArtifact;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Network;

/**
 * WARNING: Work in progress. Does not work yet.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerInfinispanServerDriver extends InfinispanServerDriver {
   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ContainerInfinispanServerDriver.class);
   public static final String INFINISPAN_SERVER_HOME = "/opt/infinispan";
   public static final int JMX_PORT = 9999;
   private final List<GenericContainer> containers;
   private static final String EXTRA_LIBS = "org.infinispan.test.server.extension.libs";
   private String name;
   CountdownLatchLoggingConsumer latch;
   ImageFromDockerfile image;
   private File rootDir;
   private final boolean preferContainerExposedPorts = Boolean.getBoolean("org.infinispan.test.server.container.preferContainerExposedPorts");

   protected ContainerInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(
            configuration,
            getDockerBridgeAddress()
      );
      this.containers = new ArrayList<>(configuration.numServers());
   }

   static InetAddress getDockerBridgeAddress() {
      DockerClient dockerClient = DockerClientFactory.instance().client();
      Network bridge = dockerClient.inspectNetworkCmd().withNetworkId("bridge").exec();
      String gateway = bridge.getIpam().getConfig().get(0).getGateway();
      return Exceptions.unchecked(() -> InetAddress.getByName(gateway));
   }

   @Override
   protected void start(String name, File rootDir, String configurationFile) {
      this.name = name;
      this.rootDir = rootDir;
      // Build a skeleton server layout
      createServerHierarchy(rootDir);
      String baseImageName = System.getProperty("org.infinispan.test.server.baseImageName", "jboss/base-jdk:11");
      Path serverOutputDir = Paths.get(System.getProperty("server.output.dir"));

      List<String> args = new ArrayList<>();
      args.add("bin/server.sh");
      args.add("-c");
      args.add(configurationFile);
      args.add("-b");
      args.add("SITE_LOCAL");
      args.add("-Djgroups.tcp.address=SITE_LOCAL");
      args.add("-Dinfinispan.cluster.name=" + name);
      args.add("-D" + TEST_HOST_ADDRESS + "=" + testHostAddress.getHostName());
      if (configuration.isJMXEnabled()) {
         args.add("-Dcom.sun.management.jmxremote.port=" + JMX_PORT);
         args.add("-Dcom.sun.management.jmxremote.authenticate=false");
         args.add("-Dcom.sun.management.jmxremote.ssl=false");
      }
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, Paths.get(INFINISPAN_SERVER_HOME, Server.DEFAULT_SERVER_CONFIG).toString());
      properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, name);
      properties.setProperty(TEST_HOST_ADDRESS, testHostAddress.getHostName());
      configuration.properties().forEach((k, v) -> args.add("-D" + k + "=" + StringPropertyReplacer.replaceProperties((String) v, properties)));

      image = new ImageFromDockerfile()
            .withFileFromPath("build", serverOutputDir)
            .withFileFromPath("test", rootDir.toPath())
            .withFileFromPath("target", serverOutputDir.getParent())
            .withFileFromPath("src", serverOutputDir.getParent().getParent().resolve("src"))
            .withDockerfileFromBuilder(builder ->
                  builder
                        .from(baseImageName)
                        .env("INFINISPAN_SERVER_HOME", INFINISPAN_SERVER_HOME)
                        .env("INFINISPAN_VERSION", Version.getVersion())
                        .label("name", "Infinispan Server")
                        .label("version", Version.getVersion())
                        .label("release", Version.getVersion())
                        .label("architecture", "x86_64")
                        .user("root")
                        .copy("build", INFINISPAN_SERVER_HOME)
                        .copy("test", INFINISPAN_SERVER_HOME + "/server")
                        .copy("src/test/resources/bin", INFINISPAN_SERVER_HOME + "/bin")
                        .run("chown", "-R", "jboss:jboss", INFINISPAN_SERVER_HOME)
                        .workDir(INFINISPAN_SERVER_HOME)
                        .user("jboss")
                        .cmd(
                              args.toArray(new String[]{})
                        )
                        .expose(
                              11222, // Protocol endpoint
                              11221, // Memcached endpoint
                              7800,  // JGroups TCP
                              43366, // JGroups MPING
                              9999   // JMX Remoting
                        )
                        .build());
      latch = new CountdownLatchLoggingConsumer(configuration.numServers(), ".*ISPN080001.*");
      for (int i = 0; i < configuration.numServers(); i++) {
         GenericContainer container = createContainer(i, rootDir);
         containers.add(i, container);
         log.infof("Starting container %s-%d", name, i);
         container.start();
      }
      Exceptions.unchecked(() -> latch.await(10, TimeUnit.SECONDS));
   }

   private GenericContainer createContainer(int i, File rootDir) {
      GenericContainer container = new GenericContainer(image);
      // Create directories which we will bind the container to
      File serverRoot = createServerHierarchy(rootDir, Integer.toString(i),
            (hostDir, dir) -> {
               String containerDir = String.format("%s/server/%s", INFINISPAN_SERVER_HOME, dir);
               if ("lib".equals(dir)) {
                  copyArtifactsToUserLibDir(hostDir);
               }
               container.withCopyFileToContainer(MountableFile.forHostPath(hostDir.getAbsolutePath()), containerDir);
               hostDir.setWritable(true, false);
            });
      // Process any enhancers
      container
            .withLogConsumer(new JBossLoggingConsumer(LogFactory.getLogger(name)).withPrefix(Integer.toString(i)))
            .withLogConsumer(latch)
            .waitingFor(Wait.forListeningPort());
      return container;
   }

   private void copyArtifactsToUserLibDir(File libDir) {
      // Maven artifacts
      String propertyArtifacts = System.getProperty(EXTRA_LIBS);
      String[] artifacts = propertyArtifacts != null ? propertyArtifacts.replaceAll("\\s+", "").split(",") : configuration.mavenArtifacts();
      if (artifacts != null && artifacts.length > 0) {
         MavenResolvedArtifact[] archives = Maven.resolver().resolve(artifacts).withoutTransitivity().asResolvedArtifact();
         for (MavenResolvedArtifact archive : archives) {
            Exceptions.unchecked(() -> {
               Path source = archive.asFile().toPath();
               Files.copy(source, libDir.toPath().resolve(source.getFileName()));
            });
         }
      }
      // Supplied artifacts
      if (configuration.archives() != null) {
         for (JavaArchive artifact : configuration.archives()) {
            File jar = libDir.toPath().resolve(artifact.getName()).toFile();
            jar.setWritable(true, false);
            artifact.as(ZipExporter.class).exportTo(jar, true);
         }
      }
   }

   @Override
   protected void stop() {
      for (int i = 0; i < containers.size(); i++) {
         log.infof("Stopping container %s-%d", name, i);
         containers.get(i).stop();
         log.infof("Stopped container %s-%d", name, i);
      }
      containers.clear();
   }

   @Override
   public boolean isRunning(int server) {
      return containers.get(server).isRunning();
   }

   @Override
   public InetSocketAddress getServerSocket(int server, int port) {
      return new InetSocketAddress(getServerAddress(server), port);
   }

   @Override
   public InetAddress getServerAddress(int server) {
      GenericContainer container = containers.get(server);
      // We talk directly to the container, and not through forwarded addresses on localhost because of
      // https://github.com/testcontainers/testcontainers-java/issues/452
      return Exceptions.unchecked(() -> InetAddress.getByName(getIpAddressFromContainer(container)));
   }

   @Override
   public void pause(int server) {
      Container.ExecResult result = Exceptions.unchecked(() -> containers.get(server).execInContainer(INFINISPAN_SERVER_HOME + "/bin/pause.sh"));
      System.out.printf("[%d] PAUSE %s\n", server, result);
   }

   @Override
   public void resume(int server) {
      Container.ExecResult result = Exceptions.unchecked(() -> containers.get(server).execInContainer(INFINISPAN_SERVER_HOME + "/bin/resume.sh"));
      System.out.printf("[%d] RESUME %s\n", server, result);
   }

   @Override
   public void stop(int server) {
      containers.get(server).stop();
   }

   @Override
   public void kill(int server) {
      Exceptions.unchecked(() -> containers.get(server).execInContainer(INFINISPAN_SERVER_HOME + "/bin/kill.sh"));
   }

   @Override
   public void restart(int server) {
      if (isRunning(server)) {
         throw new IllegalStateException("Server " + server + " is still running");
      }
      latch = new CountdownLatchLoggingConsumer(1, ".*ISPN080001.*");
      GenericContainer container = createContainer(server, rootDir);
      containers.set(server, container);
      log.infof("Restarting container %s-%d", name, server);
      container.start();
      Exceptions.unchecked(() -> latch.await(10, TimeUnit.SECONDS));
   }

   @Override
   public void restartCluster() {
      latch = new CountdownLatchLoggingConsumer(configuration.numServers(), ".*ISPN080001.*");
      for (int i = 0; i < configuration.numServers(); i++) {
         GenericContainer container = createContainer(i, rootDir);
         containers.set(i, container);
         log.infof("Restarting container %s-%d", name, i);
         container.start();
      }
      Exceptions.unchecked(() -> latch.await(10, TimeUnit.SECONDS));
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server) {
      return Exceptions.unchecked(() -> {
         GenericContainer container = containers.get(server);
         ContainerNetwork network = container.getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next();
         JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", network.getIpAddress(), JMX_PORT));
         JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
         return jmxConnector.getMBeanServerConnection();
      });
   }

   @Override
   public RemoteCacheManager createRemoteCacheManager(ConfigurationBuilder builder) {
      if (preferContainerExposedPorts) {
         return new ContainerRemoteCacheManager(containers).wrap(builder);
      } else {
         return new RemoteCacheManager(builder.build());
      }
   }
}
