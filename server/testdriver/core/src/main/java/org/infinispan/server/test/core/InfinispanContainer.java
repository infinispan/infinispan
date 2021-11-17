package org.infinispan.server.test.core;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import java.time.Duration;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.Version;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

/**
 * InfinispanContainer is an easy way to manage an Infinispan Server container.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class InfinispanContainer extends GenericContainer<InfinispanContainer> {

   public static final String IMAGE_BASENAME = "quay.io/infinispan/server";
   public static final String DEFAULT_USERNAME = "admin";
   public static final String DEFAULT_PASSWORD = "secret";

   public InfinispanContainer() {
      this(IMAGE_BASENAME + ":" + Version.getMajorMinor());
   }

   /**
    * Create an Infinispan Container by passing the full docker image name
    *
    * @param dockerImageName Full docker image name as a {@link String}, like: quay.io/infinispan/server:13.0
    */
   public InfinispanContainer(String dockerImageName) {
      this(DockerImageName.parse(dockerImageName));
      withUser(DEFAULT_USERNAME);
      withPassword(DEFAULT_PASSWORD);
   }

   /**
    * Create an Infinispan Container by passing the full docker image name
    *
    * @param dockerImageName Full docker image name as a {@link DockerImageName}, like: DockerImageName.parse("quay.io/infinispan/server:13.0")
    */
   public InfinispanContainer(final DockerImageName dockerImageName) {
      super(dockerImageName);

      logger().info("Starting an Infinispan container using [{}]", dockerImageName);
      withNetworkAliases("infinispan-" + Base58.randomString(6));
      addExposedPorts(
            11222, // HTTP/Hot Rod
            7800  // JGroups TCP
      );
      setWaitStrategy(new HttpWaitStrategy()
            .forPort(11222)
            .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
            .withStartupTimeout(Duration.ofMinutes(2)));
   }

   /**
    * Define the Infinispan username to set.
    *
    * @param user username to set
    * @return this
    */
   public InfinispanContainer withUser(String user) {
      withEnv("USER", user);
      return this;
   }

   /**
    * Define the Infinispan password to set.
    *
    * @param password Password to set
    * @return this
    */
   public InfinispanContainer withPassword(String password) {
      withEnv("PASS", password);
      return this;
   }

   /**
    * One or more artifacts to deploy in Infinispan Server's <tt>server/lib</tt> directory.
    * Artifacts can be specified as URLs or as Maven coordinates (e.g. org.postgresql:postgresql:42.3.1)
    *
    * @param artifacts
    * @return
    */
   public InfinispanContainer withArtifacts(String... artifacts) {
      if (artifacts != null || artifacts.length > 0) {
         withEnv("SERVER_LIBS", String.join(" ", artifacts));
      }
      return this;
   }

   /**
    * Creates a {@link RemoteCacheManager} configured to connect to the containerized server
    * @return
    */
   public RemoteCacheManager getRemoteCacheManager() {
      return getRemoteCacheManager(new ConfigurationBuilder());
   }

   public RemoteCacheManager getRemoteCacheManager(ConfigurationBuilder builder) {
      builder.addServer().host(getHost()).port(getMappedPort(11222))
            .security().authentication().username(getEnvMap().get("USER")).password(getEnvMap().get("PASS"));
      return new RemoteCacheManager(builder.build());
   }


}
