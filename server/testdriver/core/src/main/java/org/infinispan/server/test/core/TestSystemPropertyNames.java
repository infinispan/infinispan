package org.infinispan.server.test.core;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class TestSystemPropertyNames {
   /**
    * Specifies the name of the base image to use for the server container
    */
   public static final String INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME = "org.infinispan.test.server.container.baseImageName";
   /**
    * Specifies whether to use the ports exposed by the container
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_PREFER_CONTAINER_EXPOSED_PORTS = "org.infinispan.test.server.container.preferContainerExposedPorts";
   /**
    * Specifies whether the base image contains a prebuilt server to use instead of using the one built locally
    */
   public static final String INFINISPAN_TEST_SERVER_PRESERVE_IMAGE = "org.infinispan.test.server.container.preserveImage";
   /**
    * Specifies a comma-separated list of extra libraries (jars) to deploy into the server/lib directory
    */
   public static final String INFINISPAN_TEST_SERVER_EXTRA_LIBS = "org.infinispan.test.server.extension.libs";
   /**
    * The driver to use for running the server tests. Will override the default driver. Can be either EMBEDDED or CONTAINER
    */
   public static final String INFINISPAN_TEST_SERVER_DRIVER = "org.infinispan.test.server.driver";
   /**
    * The timeout in seconds to wait for the embedded driver servers to start
    */
   public static final String INFINISPAN_TEST_SERVER_EMBEDDED_TIMEOUT_SECONDS = "org.infinispan.test.server.embedded.timeoutSeconds";
   /**
    * The timeout in seconds to wait for the container driver servers to start
    */
   public static final String INFINISPAN_TEST_SERVER_CONTAINER_TIMEOUT_SECONDS = "org.infinispan.test.server.container.timeoutSeconds";
   /**
    * The path to a local installation of the server
    */
   public static final String INFINISPAN_TEST_SERVER_DIR = "org.infinispan.test.server.dir";
}
