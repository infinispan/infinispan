package org.infinispan.server.test;

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
   public static final String INFINISPAN_TEST_SERVER_PREBUILT = "org.infinispan.test.server.container.usePrebuiltServer";
   /**
    * Specifies whether the base image contains a prebuilt server to use instead of using the one built locally
    */
   public static final String INFINISPAN_TEST_SERVER_PRESERVE_IMAGE = "org.infinispan.test.server.container.preserveImage";
   /**
    * Specifies a comma-separated list of extra libraries (jars) to deploy into the server/lib directory
    */
   public static final String EXTRA_LIBS = "org.infinispan.test.server.extension.libs";
   /**
    * The driver to use for running the server tests. Will override the default driver. Can be either EMBEDDED or CONTAINER
    */
   public static final String INFINISPAN_TEST_SERVER_DRIVER = "org.infinispan.test.server.driver";

   /**
    * Specifies wheter the server will run external (outside the project)
    */
   public static final String INFINISPAN_TEST_SERVER_EXTERNAL = "org.infinispan.test.server.external";

}
