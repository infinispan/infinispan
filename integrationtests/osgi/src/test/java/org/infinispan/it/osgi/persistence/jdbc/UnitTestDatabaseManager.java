package org.infinispan.it.osgi.persistence.jdbc;

import org.infinispan.persistence.jdbc.Dialect;
import org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfigurationBuilder;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that assures concurrent access to the in memory database.
 *
 * @author Mircea.Markus@jboss.com
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 * @author Tristan Tarrant
 */

public class UnitTestDatabaseManager {

   private static AtomicInteger userIndex = new AtomicInteger(0);
   private static final String H2_DRIVER = org.h2.Driver.class.getName();
   private static final Dialect dt;

   static {
      String driver = "";
      try {
         driver = H2_DRIVER;
         dt = Dialect.H2;
         try {
            Class.forName(driver);
         } catch (ClassNotFoundException e) {
            driver = H2_DRIVER;
            Class.forName(H2_DRIVER);
         }
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }
   
   public static void setDialect(AbstractJdbcStoreConfigurationBuilder<?, ?> builder) {
      builder.dialect(dt);
   }

   public static ConnectionFactoryConfigurationBuilder<?> configureUniqueConnectionFactory(AbstractJdbcStoreConfigurationBuilder<?, ?> store) {
      return store
            .connectionPool()
            .driverClass(org.h2.Driver.class)
            .connectionUrl(String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", extractTestName() + userIndex.incrementAndGet()))
            .username("sa");
   }

   private static String extractTestName() {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      if (stack.length == 0)
         return null;
      for (int i = stack.length - 1; i > 0; i--) {
         StackTraceElement e = stack[i];
         String className = e.getClassName();
         if (className.indexOf("org.infinispan") != -1)
            return className.replace('.', '_') + "_" + e.getMethodName();
      }
      return null;
   }

   public static void buildTableManipulation(TableManipulationConfigurationBuilder<?, ?> table, boolean binary) {
      table.tableNamePrefix(binary ? "ISPN_BINARY" : "ISPN_STRING")
         .idColumnName("ID_COLUMN")
         .idColumnType(binary ? "INT" : "VARCHAR(255)")
         .dataColumnName("DATA_COLUMN")
         .dataColumnType("BLOB")
         .timestampColumnName("TIMESTAMP_COLUMN")
         .timestampColumnType("BIGINT");
   }
}
