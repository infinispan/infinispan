package org.infinispan.server.persistence;

import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.core.persistence.DatabaseServerListener;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 10.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({
      PooledConnectionOperations.class,
      ManagedConnectionOperations.class,
      JdbcStringBasedCacheStoreIT.class,
      AsyncJdbcStringBasedCacheStore.class
})
public class PersistenceIT extends InfinispanSuite {

   static final String DATABASE_LIBS = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_LIBS);
   static final String EXTERNAL_JDBC_DRIVER = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_EXTERNAL_DRIVERS);
   static final String JDBC_DRIVER_FROM_FILE = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_CONTAINER_DATABASE_DRIVERS_FILE, "target/test-classes/database/jdbc-drivers.txt");

   public static String[] DEFAULT_DATABASES = new String[]{"h2", "mysql", "postgres"};

   public static final DatabaseServerListener DATABASE_LISTENER = new DatabaseServerListener(DEFAULT_DATABASES);

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config(System.getProperty(PersistenceIT.class.getName(), "configuration/PersistenceTest.xml"))
                                    .numServers(1)
                                    .runMode(ServerRunMode.CONTAINER)
                                    .mavenArtifacts(getJdbcDrivers())
                                    .artifacts(getJavaArchive())
                                    .addListener(DATABASE_LISTENER)
                                    .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
                                    .build();

   public static String[] getJdbcDrivers() {
      Map<String, String> jdbcDrivers = Exceptions.unchecked(() -> Files.lines(Paths.get(JDBC_DRIVER_FROM_FILE))
              .collect(Collectors.toMap(PersistenceIT::getArtifactId, Function.identity())));

      // default jdbc drivers can also be set through system properties
      // jdbc drivers can be updated or added
      if(DATABASE_LIBS != null) {
         Arrays.stream(DATABASE_LIBS.split(",")).forEach(it -> jdbcDrivers.put(getArtifactId(it), it));
      }

      return jdbcDrivers.values().toArray(new String[0]);
   }

   private static String getArtifactId(String gav) {
      return gav.split(":")[1];
   }

   //Some jdbc drivers are not available through maven (like sybase), in this case we can pass the jdbc driver location
   public static JavaArchive[] getJavaArchive() {
      List<JavaArchive> externalJdbcDriver = new ArrayList<>();

      if(EXTERNAL_JDBC_DRIVER != null) {
         Arrays.stream(EXTERNAL_JDBC_DRIVER.split(","))
                 .map(File::new)
                 .forEach( it -> externalJdbcDriver.add(ShrinkWrap.createFromZipFile(JavaArchive.class, it)));
      }

      return externalJdbcDriver.toArray(new JavaArchive[0]);
   }
}
