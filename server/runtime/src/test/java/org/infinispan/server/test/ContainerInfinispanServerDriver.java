package org.infinispan.server.test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.infinispan.commons.util.Version;
import org.infinispan.test.Exceptions;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Network;

/**
 * WARNING: Work in progress. Does not work yet.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContainerInfinispanServerDriver extends InfinispanServerDriver {
   public static final String INFINISPAN_SERVER_HOME = "/opt/infinispan";
   private final List<GenericContainer> containers;

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
      args.add("-Dcom.sun.management.jmxremote.port=9999");
      args.add("-Dcom.sun.management.jmxremote.authenticate=false");
      args.add("-Dcom.sun.management.jmxremote.ssl=false");
      configuration.properties().forEach((k, v) -> {
         args.add("-D" + k + "=" + v);
      });

      ImageFromDockerfile image = new ImageFromDockerfile()
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
                        .user("jboss")
                        .copy("build", INFINISPAN_SERVER_HOME)
                        .copy("test", INFINISPAN_SERVER_HOME + "/server")
                        .copy("src/test/resources/bin", INFINISPAN_SERVER_HOME + "/bin")
                        .workDir(INFINISPAN_SERVER_HOME)
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
      CountdownLatchLoggingConsumer latch;
      if (configuration.numServers() > 1) {
         latch = new CountdownLatchLoggingConsumer(configuration.numServers(), ".*ISPN080001.*");
      } else {
         latch = new CountdownLatchLoggingConsumer(1, ".*ISPN080001.*");
      }
      for (int i = 0; i < configuration.numServers(); i++) {
         GenericContainer container = new GenericContainer(image);

         // Create directories which we will bind the container to
         createServerHierarchy(rootDir, Integer.toString(i),
               (hostDir, dir) -> {
                  String containerDir = String.format("%s/server/%s", INFINISPAN_SERVER_HOME, dir);
                  container.withFileSystemBind(hostDir.getAbsolutePath(), containerDir);
                  hostDir.setWritable(true, false);
               });
         containers.add(container);
         container
               .withLogConsumer(new JBossLoggingConsumer(LogFactory.getLogger(name)).withPrefix(Integer.toString(i)))
               .withLogConsumer(latch)
               .waitingFor(Wait.forListeningPort())
               .start();
      }
      Exceptions.unchecked(() -> latch.await(10, TimeUnit.SECONDS));
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
      InspectContainerResponse containerInfo = container.getContainerInfo();
      ContainerNetwork network = containerInfo.getNetworkSettings().getNetworks().values().iterator().next();
      // We talk directly to the container, and not through forwarded addresses on localhost because of
      // https://github.com/testcontainers/testcontainers-java/issues/452
      return new InetSocketAddress(network.getIpAddress(), port);
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
   public MBeanServerConnection getJmxConnection(int server) {
      return Exceptions.unchecked(() -> {
         GenericContainer container = containers.get(server);
         JMXServiceURL url = new JMXServiceURL("service:jmx:");
         JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
         return jmxConnector.getMBeanServerConnection();
      });
   }
}
