/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.loaders.jdbc;

import org.infinispan.io.ByteBuffer;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.marshall.StreamingMarshaller;
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
