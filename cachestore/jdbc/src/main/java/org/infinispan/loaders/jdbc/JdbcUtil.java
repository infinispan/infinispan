package org.infinispan.loaders.jdbc;

import org.infinispan.io.ByteBuffer;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Contains common methods used by jdbc CacheStores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcUtil {

   private static Log log = LogFactory.getLog(JdbcUtil.class);

   public static void safeClose(Statement ps) {
      if (ps != null) {
         try {
            ps.close();
         } catch (SQLException e) {
            e.printStackTrace();
         }
      }
   }

   public static void safeClose(Connection connection) {
      if (connection != null) {
         try {
            connection.close();
         } catch (SQLException e) {
            e.printStackTrace();
         }
      }
   }

   public static void safeClose(ResultSet rs) {
      if (rs != null) {
         try {
            rs.close();
         } catch (SQLException e) {
            e.printStackTrace();
         }
      }
   }

   public static ByteBuffer marshall(Marshaller marshaller, Object bucket) throws CacheLoaderException {
      try {
         return marshaller.objectToBuffer(bucket);
      } catch (IOException e) {
         String message = "I/O failure while marshalling " + bucket;
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      }
   }

   public static Object unmarshall(Marshaller marshaller, InputStream inputStream) throws CacheLoaderException {
      try {
         return marshaller.objectFromStream(inputStream);
      } catch (IOException e) {
         String message = "I/O error while unmarshalling from stram";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } catch (ClassNotFoundException e) {
         String message = "*UNEXPECTED* ClassNotFoundException. This should not happen as Bucket class exists";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      }
   }
}
