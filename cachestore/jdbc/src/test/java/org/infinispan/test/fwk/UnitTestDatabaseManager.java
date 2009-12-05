/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.test.fwk;

import org.infinispan.loaders.jdbc.JdbcUtil;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
         Class.forName("org.h2.Driver");
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
      realConfig.setDriverClass("org.h2.Driver");
      realConfig.setConnectionUrl("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1");
      realConfig.setConnectionFactoryClass(PooledConnectionFactory.class.getName());
      realConfig.setUserName("sa");
   }

   public static ConnectionFactoryConfig getUniqueConnectionFactoryConfig() {
      synchronized (realConfig) {
         return returnBasedOnDifferentInstance();
      }
   }

   public static void shutdownInMemoryDatabase(ConnectionFactoryConfig config) {

      

//      Connection conn = null;
//      Statement st = null;
//      try {
//         String shutDownConnection = getShutdownUrl(config);
//         String url = config.getConnectionUrl();
//         assert url != null;
//         try {
//            conn = DriverManager.getConnection(shutDownConnection);
//         } catch (SQLException e) {
//            //expected
//         }
////         st = conn.createStatement();
////         st.execute("SHUTDOWN");
//      }
//      catch (Throwable e) {
//         throw new IllegalStateException(e);
//      }
//      finally {
////         try {
//////            conn.close();
//////            st.close();
////         }
////         catch (SQLException e) {
////            e.printStackTrace();
////         }
//      }

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
      assert found : String.format("%1s not found in %2s", pattern, jdbcUrl);
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

      return new TableManipulation("ID_COLUMN", "VARCHAR(255)", "ISPN_JDBC", "DATA_COLUMN",
                                   "BLOB", "TIMESTAMP_COLUMN", "BIGINT");

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
