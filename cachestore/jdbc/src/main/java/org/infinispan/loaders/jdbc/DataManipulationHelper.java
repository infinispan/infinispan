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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The purpose of this class is to factorize the repeating code between {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}
 * and {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}. This class implements GOF's template method pattern.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class DataManipulationHelper {

   private static final Log log = LogFactory.getLog(DataManipulationHelper.class);

   private final ConnectionFactory connectionFactory;
   private final TableManipulation tableManipulation;
   protected StreamingMarshaller marshaller;


   public DataManipulationHelper(ConnectionFactory connectionFactory, TableManipulation tableManipulation, StreamingMarshaller marshaller) {
      this.connectionFactory = connectionFactory;
      this.tableManipulation = tableManipulation;
      this.marshaller = marshaller;
   }

   public void clear() throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         String sql = tableManipulation.getDeleteAllRowsSql();
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         int result = ps.executeUpdate();
         if (log.isTraceEnabled()) {
            log.trace("Successfully removed " + result + " rows.");
         }
      } catch (SQLException ex) {
         logAndThrow(ex, "Failed clearing JdbcBinaryCacheStore");
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }


   public final void fromStreamSupport(ObjectInput objectInput) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = tableManipulation.getInsertRowSql();
         ps = conn.prepareStatement(sql);

         int readCount = 0;
         int batchSize = tableManipulation.getBatchSize();

         Object objFromStream = marshaller.objectFromObjectStream(objectInput);
         while (fromStreamProcess(objFromStream, ps, objectInput)) {
            ps.addBatch();
            readCount++;
            if (readCount % batchSize == 0) {
               ps.executeBatch();
               if (log.isTraceEnabled()) {
                  log.trace("Executing batch " + readCount / batchSize + ", batch size is " + batchSize);
               }
            }
            objFromStream = marshaller.objectFromObjectStream(objectInput);
         }
         if (readCount % batchSize != 0) {
            ps.executeBatch();//flush the batch
         }
         if (log.isTraceEnabled()) {
            log.trace("Successfully inserted " + readCount + " buckets into the database, batch size is " + batchSize);
         }
      } catch (IOException ex) {
         logAndThrow(ex, "I/O failure while integrating state into store");
      } catch (SQLException e) {
         logAndThrow(e, "SQL failure while integrating state into store");
      } catch (ClassNotFoundException e) {
         logAndThrow(e, "Unexpected failure while integrating state into store");
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled()) log.trace("Interrupted while reading from stream"); 
         Thread.currentThread().interrupt();
      } finally {
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }


   public final void toStreamSupport(ObjectOutput objectOutput, byte streamDelimiter, boolean filterExpired) throws CacheLoaderException {
      //now write our data
      Connection connection = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = filterExpired ? tableManipulation.getLoadNonExpiredAllRowsSql() : tableManipulation.getLoadAllRowsSql();
         if (log.isTraceEnabled()) {
            log.trace("Running sql '" + sql);
         }
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         if (filterExpired) {
            ps.setLong(1, System.currentTimeMillis());
         }
         rs = ps.executeQuery();
         rs.setFetchSize(tableManipulation.getFetchSize());
         while (rs.next()) {
            InputStream is = rs.getBinaryStream(1);
            toStreamProcess(rs, is, objectOutput);
         }
         marshaller.objectToObjectStream(streamDelimiter, objectOutput);
      } catch (SQLException e) {
         logAndThrow(e, "SQL Error while storing string keys to database");
      } catch (IOException e) {
         logAndThrow(e, "I/O Error while storing string keys to database");
      }
      finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(connection);
      }
   }

   public final Set<InternalCacheEntry> loadAllSupport(boolean filterExpired) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = filterExpired ? tableManipulation.getLoadNonExpiredAllRowsSql() : tableManipulation.getLoadAllRowsSql();
         if (log.isTraceEnabled()) {
            log.trace("Running sql '" + sql);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         if (filterExpired) {
            ps.setLong(1, System.currentTimeMillis());
         }
         rs = ps.executeQuery();
         rs.setFetchSize(tableManipulation.getFetchSize());
         Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(tableManipulation.getFetchSize());
         while (rs.next()) {
            loadAllProcess(rs, result);
         }
         return result;
      } catch (SQLException e) {
         String message = "SQL error while fetching all StoredEntries";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   public Set<Object> loadAllKeysSupport(Set<Object> keysToExclude) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {

         String sql = getLoadAllKeysSql();
         if (log.isTraceEnabled()) {
            log.trace("Running sql '" + sql);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         rs = ps.executeQuery();
         rs.setFetchSize(tableManipulation.getFetchSize());
         Set<Object> result = new HashSet<Object>(tableManipulation.getFetchSize());
         while (rs.next()) {
            loadAllKeysProcess(rs, result, keysToExclude);
         }
         return result;
      } catch (SQLException e) {
         String message = "SQL error while fetching all StoredEntries";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   public final Set<InternalCacheEntry> loadSome(int maxEntries) throws CacheLoaderException {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         String sql = tableManipulation.getLoadSomeRowsSql();
         if (log.isTraceEnabled()) {
            log.trace("Running sql '" + sql);
         }
         conn = connectionFactory.getConnection();
         if (tableManipulation.isVariableLimitSupported()) {
            ps = conn.prepareStatement(sql);
            ps.setInt(1, maxEntries);
         } else {
            ps = conn.prepareStatement(sql.replace("?", String.valueOf(maxEntries)));
         }
         rs = ps.executeQuery();
         rs.setFetchSize(tableManipulation.getFetchSize());
         Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(maxEntries);
         while (rs.next()) {
            loadAllProcess(rs, result);
         }
         return result;
      } catch (SQLException e) {
         String message = "SQL error while fetching all StoredEntries";
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);
         connectionFactory.releaseConnection(conn);
      }
   }

   protected boolean includeKey(Object key, Set<Object> keysToExclude) {
      return keysToExclude == null || !keysToExclude.contains(key);
   }

   protected abstract String getLoadAllKeysSql();

   protected abstract void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result) throws SQLException, CacheLoaderException;

   protected abstract void loadAllKeysProcess(ResultSet rs, Set<Object> keys, Set<Object> keysToExclude) throws SQLException, CacheLoaderException;

   protected abstract void toStreamProcess(ResultSet rs, InputStream is, ObjectOutput objectOutput) throws CacheLoaderException, SQLException, IOException;

   protected abstract boolean fromStreamProcess(Object objFromStream, PreparedStatement ps, ObjectInput objectInput)
         throws SQLException, CacheLoaderException, IOException, ClassNotFoundException, InterruptedException;

   public static void logAndThrow(Exception e, String message) throws CacheLoaderException {
      log.error(message, e);
      throw new CacheLoaderException(message, e);
   }
}
