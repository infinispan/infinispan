package org.infinispan.persistence.jdbc.common;

import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.StreamAwareMarshaller;

/**
 * Contains common methods used by JDBC CacheStores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcUtil {
   public static void safeClose(Statement ps) {
      if (ps != null) {
         try {
            ps.close();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureUnexpected(e);
         }
      }
   }

   public static void safeClose(Connection connection) {
      if (connection != null) {
         try {
            connection.close();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureClosingConnection(connection, e);
         }
      }
   }

   public static void safeClose(ResultSet rs) {
      if (rs != null) {
         try {
            rs.close();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureUnexpected(e);
         }
      }
   }

   public static ByteBuffer marshall(Object obj, Marshaller marshaller) {
      try {
         return marshaller.objectToBuffer(obj);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new MarshallingException(e);
      } catch (IOException e) {
         PERSISTENCE.errorMarshallingObject(e, obj);
         throw new MarshallingException("I/O failure while marshalling object: " + obj, e);
      }
   }

   @SuppressWarnings("unchecked")
   public static <T> T unmarshall(InputStream inputStream, StreamAwareMarshaller marshaller) {
      try {
         return (T) marshaller.readObject(inputStream);
      } catch (IOException e) {
         PERSISTENCE.ioErrorUnmarshalling(e);
         throw new MarshallingException("I/O error while unmarshalling from stream", e);
      } catch (ClassNotFoundException e) {
         PERSISTENCE.unexpectedClassNotFoundException(e);
         throw new MarshallingException(e);
      }
   }

   @SuppressWarnings("unchecked")
   public static <T> T unmarshall(ByteBuffer buf, Marshaller marshaller) {
      try {
         return (T) marshaller.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
      } catch (IOException e) {
         throw new MarshallingException("I/O error while unmarshalling", e);
      } catch (ClassNotFoundException e) {
         PERSISTENCE.unexpectedClassNotFoundException(e);
         throw new MarshallingException(e);
      }
   }
}
