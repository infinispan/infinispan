package org.infinispan.persistence.jdbc.common;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.SimpleConnectionFactory;

/**
 * Class that assures concurrent access to the in memory database.
 *
 * @author Mircea.Markus@jboss.com
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 * @author Tristan Tarrant
 */

public class UnitTestDatabaseManager {
   private static final AtomicInteger userIndex = new AtomicInteger(0);
   private static final String DB_TYPE = System.getProperty("infinispan.test.jdbc.db", "H2");
   private static final String H2_DRIVER = org.h2.Driver.class.getName();
   private static final String NON_EXISTENT_DRIVER = "non.existent.Driver";
   private static final DatabaseType dt;

   static {
      String driver;
      try {
         if ("mysql".equalsIgnoreCase(DB_TYPE)) {
            driver = com.mysql.jdbc.Driver.class.getName();
            dt = DatabaseType.MYSQL;
         } else {
            driver = H2_DRIVER;
            dt = DatabaseType.H2;
         }
         try {
            Class.forName(driver);
         } catch (ClassNotFoundException e) {
            Class.forName(H2_DRIVER);
         }
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   public static ConnectionFactoryConfigurationBuilder<?> configureUniqueConnectionFactory(AbstractJdbcStoreConfigurationBuilder<?, ?> store) {
      switch (dt) {
      case H2:
         return store
            .connectionPool()
               .driverClass(org.h2.Driver.class)
               .connectionUrl(String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", extractTestName() + userIndex.incrementAndGet()))
               .username("sa");
      case MYSQL:
         return store
            .simpleConnection()
               .driverClass(com.mysql.jdbc.Driver.class)
               .connectionUrl("jdbc:mysql://localhost/infinispan?user=ispn&password=ispn")
               .username("ispn")
               .password("ispn");
      default:
         throw new RuntimeException("Cannot configure connection for database type "+dt);
      }
   }

   public static ConnectionFactoryConfigurationBuilder<?> configureSimpleConnectionFactory(AbstractJdbcStoreConfigurationBuilder<?, ?> store) {
      return store
            .simpleConnection()
               .driverClass(org.h2.Driver.class)
               .connectionUrl(String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", extractTestName() + userIndex.incrementAndGet()))
               .username("sa");
   }

   public static ConnectionFactoryConfigurationBuilder<?> configureBrokenConnectionFactory(AbstractJdbcStoreConfigurationBuilder<?, ?> storeBuilder) {
      return storeBuilder.connectionPool()
                  .driverClass(NON_EXISTENT_DRIVER);
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

   public static void setDialect(AbstractJdbcStoreConfigurationBuilder builder) {
      builder.dialect(dt);
   }

   public static void verifyConnectionLeaks(ConnectionFactory connectionFactory) {
      if (connectionFactory instanceof PooledConnectionFactory) {
         PooledConnectionFactory pcf = (PooledConnectionFactory) connectionFactory;
         try {
            Thread.sleep(500); // C3P0 needs a little delay before reporting the correct number of connections. Bah!
            assertEquals(pcf.getActiveConnections(), 0);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      } else if (connectionFactory instanceof SimpleConnectionFactory) {
         SimpleConnectionFactory scf = (SimpleConnectionFactory) connectionFactory;
         assertEquals(scf.getConnectionCount(), 0);
      }
   }

}
