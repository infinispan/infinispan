package org.infinispan.loaders.jdbc;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.loaders.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Contains common methods used by JDBC CacheStores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcUtil {

   private static final Log log = LogFactory.getLog(JdbcUtil.class, Log.class);

   public static void safeClose(Statement ps) {
      if (ps != null) {
         try {
            ps.close();
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }
      }
   }

   public static void safeClose(Connection connection) {
      if (connection != null) {
         try {
            connection.close();
         } catch (SQLException e) {
            log.sqlFailureClosingConnection(connection, e);
         }
      }
   }

   public static void safeClose(ResultSet rs) {
      if (rs != null) {
         try {
            rs.close();
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }
      }
   }

   public static ByteBuffer marshall(StreamingMarshaller marshaller, Object bucket) throws CacheLoaderException, InterruptedException {
      try {
         return marshaller.objectToBuffer(bucket);
      } catch (IOException e) {
         log.errorMarshallingBucket(e, bucket);
         throw new CacheLoaderException("I/O failure while marshalling bucket: " + bucket, e);
      }
   }

   public static Object unmarshall(StreamingMarshaller marshaller, InputStream inputStream) throws CacheLoaderException {
      try {
         return marshaller.objectFromInputStream(inputStream);
      } catch (IOException e) {
         log.ioErrorUnmarshalling(e);
         throw new CacheLoaderException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         log.unexpectedClassNotFoundException(e);
         throw new CacheLoaderException("*UNEXPECTED* ClassNotFoundException. This should not happen as Bucket class exists", e);
      }
   }
}
