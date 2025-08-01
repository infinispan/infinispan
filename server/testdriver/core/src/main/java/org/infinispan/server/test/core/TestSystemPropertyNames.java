package org.infinispan.server.test.core;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class TestSystemPropertyNames {

   public static final String PREFIX = "org.infinispan.test.server.";

   /**
    * Specifies the name of the base image to use for the server container
    */
   public static final String INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME = PREFIX + "container.baseImageName";
   /**
    * Specifies the name to be used when creating an image from a snapshot directory. This is only used when
    * {@link #INFINISPAN_TEST_SERVER_DIR} is provided. Note this is not required and a sensible default is used when not done.
    */
   public static final String INFINISPAN_TEST_SERVER_SNAPSHOT_IMAGE_NAME = PREFIX + "container.snapshotImageName";
   /**
    * Specifies whether the base image contains a prebuilt server to use instead of using the one built locally
    */
   public static final String INFINISPAN_TEST_SERVER_PRESERVE_IMAGE = PREFIX + "container.preserveImage";
   /**
    * Specifies a comma-separated list of extra libraries (jars) to deploy into the server/lib directory
    */
   public static final String INFINISPAN_TEST_SERVER_EXTRA_LIBS = PREFIX + "extension.libs";
   /**
    * The driver to use for running the server tests. Will override the default driver. Can be either EMBEDDED or CONTAINER
    */
   public static final String INFINISPAN_TEST_SERVER_DRIVER = PREFIX + "driver";
   /**
    * The timeout in seconds to wait for the embedded driver servers to start
    */
   public static final String INFINISPAN_TEST_SERVER_EMBEDDED_TIMEOUT_SECONDS = PREFIX + "embedded.timeoutSeconds";
   /**
    * The timeout in seconds to wait for the container driver servers to start
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_TIMEOUT_SECONDS = PREFIX + "container.timeoutSeconds";
   /**
    * The timeout in seconds to wait for the container driver servers to start
    */
   public static final String INFINISPAN_TEST_SERVER_FORKED_TIMEOUT_SECONDS = PREFIX + "container.timeoutSeconds";

   /**
    * The path to a local installation of the server
    */
   public static final String INFINISPAN_TEST_SERVER_DIR = PREFIX + "dir";
   /**
    * The version to use for the Infinispan Server Container. This is used for informational purposes only, unless there
    * is no container or server directory specified. If so then a container from quay.io matching this version will be used.
    */
   public static final String INFINISPAN_TEST_SERVER_VERSION = PREFIX + "version";

   /**
    * Boolean property to notify the driver that the join timeout should not be skipped. This is useful when running
    * more than one driver without xsite so that they can still join together with a merge.
    */
   public static final String INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT = PREFIX + "requireJoinTimeout";
   /**
    * The log file for the container server driver
    * It should be a file name located in the module server/tests/src/test/resources/configuration or a full path
    */
   public static final String INFINISPAN_TEST_SERVER_LOG_FILE = PREFIX + "container.logFile";
   /**
    * The maximum amount of memory the container can use.
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_MEMORY = PREFIX + "container.memory";
   /**
    * The amount of memory this container is allowed to swap to disk
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_MEMORY_SWAP = PREFIX + "container.memorySwap";
   /**
    * LogMessageWaitStrategy pattern to use during container initialization
    */
   public static final String INFINISPAN_TEST_CONTAINER_DATABASE_LOG_MESSAGE = "org.infinispan.test.database.container.log.regex";
   /**
    * Specifies a different set of databases, default is H2, MySQL and PostgreSQL
    */
   public static final String INFINISPAN_TEST_CONTAINER_DATABASE_TYPES = "org.infinispan.test.database.types";
   /**
    * Specifies an external database configuration, default is src/test/resources/database/id.properties
    */
   public static final String INFINISPAN_TEST_CONTAINER_DATABASE_PROPERTIES = "org.infinispan.test.database.properties.path";
   /**
    * JDBC drivers to the servers in order to run tests against specific databases. Default is H2, MySQL and PostgreSQL
    */
   public static final String INFINISPAN_TEST_CONTAINER_DATABASE_LIBS = "org.infinispan.test.database.jdbc.drivers";
   /**
    * Convenient way to set GAV jdbc drivers from a file
    */
   public static final String INFINISPAN_TEST_CONTAINER_DATABASE_DRIVERS_FILE = "org.infinispan.test.database.jdbc.drivers.file";
   /**
    * Specifies external JDBC drivers which is not find into maven repo
    */
   public static final String INFINISPAN_TEST_CONTAINER_DATABASE_EXTERNAL_DRIVERS = "org.infinispan.test.database.jdbc.drivers.external";
   /**
    * Specifies the number of the container to start with JVM debug options (port 5005)
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_DEBUG = PREFIX + "container.debug";

   /**
    * Specifies whether a docker volume should be created and mounted to the container
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED= PREFIX + "container.volume";

   /**
    * Specifies the soft and hard ulimit to use in the container.
    * Follows the format: `<code>soft,hard</code>`
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_ULIMIT = PREFIX + "container.ulimit";

   private static final String INFINISPAN_ROLLING_UPGRADE_TEST = PREFIX + "rolling.upgrade.";

   /**
    * Specifies the version to start the containers during the rolling upgrade procedure.
    */
   public static final String INFINISPAN_ROLLING_UPGRADE_FROM_VERSION = INFINISPAN_ROLLING_UPGRADE_TEST + "from";

   /**
    * Specifies the target version to perform the rolling upgrades.
    */
   public static final String INFINISPAN_ROLLING_UPGRADE_TO_VERSION = INFINISPAN_ROLLING_UPGRADE_TEST + "to";

   /**
    * Specifies the name of the keycloak base image
    */
   public static final String KEYCLOAK_IMAGE = "org.infinispan.test.keycloakImage";
   /**
    * Specifies the keycloak user
    */
   public static final String KEYCLOAK_USER = "org.infinispan.test.keycloakUser";
   /**
    * Specifies the keycloak user password
    */
   public static final String KEYCLOAK_PASSWORD = "org.infinispan.test.keycloakPassword";
   /**
    * Specifies the keycloak realm
    */
   public static final String KEYCLOAK_REALM = "org.infinispan.test.keycloakRealm";
   /**
    * Specifies the name of the Jaeger image
    */
   public static final String JAEGER_IMAGE = "org.infinispan.test.jaegerImage";
   /**
    * Specifies the type of the ldap server
    */
   public static final String LDAP_SERVER = "org.infinispan.test.ldapServer";
   /**
    * Specifies the type of sasl mechanism for the Hot Rod client. Not all tests are using it.
    */
   public static final String HOTROD_CLIENT_SASL_MECHANISM = "org.infinispan.test.hotrod.client.salsmechanism";
   /**
    * The property specifying the path to jacoco exec files.
    */
   public static final String JACOCO_REPORTS_DIR = "dir.jacoco";
   /**
    * Specifies if the coverage profile is enabled. Is used for enableing jacoco agent during container run.
    */
   public static final String COVERAGE_ENABLED = "coverage.enabled";



}
