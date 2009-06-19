package org.infinispan.test.fwk;


import org.infinispan.loaders.jdbc.JdbcUtil;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that assures concurrent access to the in memory database.
 *
 * @author Mircea.Markus@jboss.com
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */

public class UnitTestDatabaseManager {
   private static final ConnectionFactoryConfig realConfig = new ConnectionFactoryConfig();

   private static AtomicInteger userIndex = new AtomicInteger(0);

   static {
      try {
         Class.forName("org.hsqldb.jdbcDriver");
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
      realConfig.setDriverClass("org.hsqldb.jdbcDriver");
      realConfig.setConnectionUrl("jdbc:hsqldb:mem:infinispan");
      realConfig.setConnectionFactoryClass(PooledConnectionFactory.class.getName());
      realConfig.setUserName("sa");
   }

   public static ConnectionFactoryConfig getUniqueConnectionFactoryConfig() {
      synchronized (realConfig) {
         return returnBasedOnDifferentInstance();
      }
   }

   public static void shutdownInMemoryDatabase(ConnectionFactoryConfig config) {

      Connection conn = null;
      Statement st = null;
      try {
         String shutDownConnection = getShutdownUrl(config);
         String url = config.getConnectionUrl();
         assert url != null;
         conn = DriverManager.getConnection(shutDownConnection);
         st = conn.createStatement();
         st.execute("SHUTDOWN");
      }
      catch (Throwable e) {
         throw new IllegalStateException(e);
      }
      finally {
         try {
            conn.close();
            st.close();
         }
         catch (SQLException e) {
            e.printStackTrace();
         }
      }

   }

   public static String getDatabaseName(Properties prop) {
      StringTokenizer tokenizer = new StringTokenizer(prop.getProperty("cache.jdbc.url"), ":");
      tokenizer.nextToken();
      tokenizer.nextToken();
      tokenizer.nextToken();
      return tokenizer.nextToken();
   }


   private static String getShutdownUrl(ConnectionFactoryConfig props) {
      String url = props.getConnectionUrl();
      assert url != null;
      StringTokenizer tokenizer = new StringTokenizer(url, ";");
      String result = tokenizer.nextToken() + ";" + "shutdown=true";
      return result;
   }


   private static ConnectionFactoryConfig returnBasedOnDifferentInstance() {
      ConnectionFactoryConfig result = realConfig.clone();
      String jdbcUrl = result.getConnectionUrl();
      Pattern pattern = Pattern.compile("infinispan");
      Matcher matcher = pattern.matcher(jdbcUrl);
      boolean found = matcher.find();
      assert found : String.format("%1s not found in %1s", pattern, jdbcUrl);
      String newJdbcUrl = matcher.replaceFirst(extractTestName() + userIndex.incrementAndGet());
      result.setConnectionUrl(newJdbcUrl);
      return result;
   }

   private static String extractTestName() {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      if (stack.length == 0) return null;
      for (int i = stack.length - 1; i > 0; i--) {
         StackTraceElement e = stack[i];
         String className = e.getClassName();
         if (className.indexOf("org.infinispan") != -1) return className.replace('.', '_') + "_" + e.getMethodName();
      }
      return null;
   }


   public static TableManipulation buildDefaultTableManipulation() {

      return new TableManipulation("ID_COLUMN", "VARCHAR(255)", "HORIZON_JDBC", "DATA_COLUMN",
                                   "BINARY", "TIMESTAMP_COLUMN", "BIGINT");

   }


   /**
    * Counts the number of rows in the given table.
    */


   public static int rowCount(ConnectionFactory connectionFactory, String tableName) {

      Connection conn = null;
      PreparedStatement statement = null;
      ResultSet resultSet = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = "SELECT count(*) FROM " + tableName;
         statement = conn.prepareStatement(sql);
         resultSet = statement.executeQuery();
         resultSet.next();
         return resultSet.getInt(1);
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
      finally {
         JdbcUtil.safeClose(resultSet);
         JdbcUtil.safeClose(statement);
         connectionFactory.releaseConnection(conn);
      }
   }

}
