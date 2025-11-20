package org.infinispan.testcontainers;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import java.time.Duration;

import org.infinispan.commons.util.Version;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;

/**
 * InfinispanContainer is an easy way to manage an Infinispan Server container.
 *
 * @since 16.0
 **/
public class InfinispanContainer extends GenericContainer<InfinispanContainer> {

   public static final int DEFAULT_HOTROD_PORT = 11222;
   public static final int DEFAULT_JGROUPS_TCP_PORT = 7800;
   public static final String IMAGE_BASENAME = "quay.io/infinispan/server";
   public static final String DEFAULT_USERNAME = "admin";
   public static final String DEFAULT_PASSWORD = "secret";
   public static final String PASS = "PASS";
   public static final String SERVER_LIBS = "SERVER_LIBS";
   public static final String USER = "USER";

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
            DEFAULT_HOTROD_PORT, // HTTP/Hot Rod
            DEFAULT_JGROUPS_TCP_PORT  // JGroups TCP
      );
      setWaitStrategy(new HttpWaitStrategy()
            .forPort(DEFAULT_HOTROD_PORT)
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
      withEnv(USER, user);
      return this;
   }

   /**
    * Define the Infinispan password to set.
    *
    * @param password Password to set
    * @return this
    */
   public InfinispanContainer withPassword(String password) {
      withEnv(PASS, password);
      return this;
   }

   /**
    * One or more artifacts to deploy in Infinispan Server's <code>server/lib</code> directory.
    * Artifacts can be specified as URLs or as Maven coordinates (e.g. org.postgresql:postgresql:42.3.1)
    *
    * @param artifacts
    * @return this
    */
   public InfinispanContainer withArtifacts(String... artifacts) {
      if (artifacts != null || artifacts.length > 0) {
         withEnv(SERVER_LIBS, String.join(" ", artifacts));
      }
      return this;
   }

   public String getConnectionURI() {
      String user = getEnvMap().get(USER);
      String pass = getEnvMap().get(PASS);
      return String.format("hotrod://%s:%s@%s:%s", user, pass, getContainerIpAddress(), getMappedPort(11222));
   }

}
