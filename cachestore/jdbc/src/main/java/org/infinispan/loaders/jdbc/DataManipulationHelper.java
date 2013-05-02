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
import org.infinispan.loaders.jdbc.logging.Log;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

/**
 * The purpose of this class is to factorize the repeating code between {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}
 * and {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}. This class implements GOF's template method pattern.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class DataManipulationHelper {

   private static final Log log = LogFactory.getLog(DataManipulationHelper.class, Log.class);

   private final ConnectionFactory connectionFactory;
   private final TableManipulation tableManipulation;
   protected StreamingMarshaller marshaller;
   private final TimeService timeService;


   public DataManipulationHelper(ConnectionFactory connectionFactory, TableManipulation tableManipulation, StreamingMarshaller marshaller,
                                 TimeService timeService) {
      this.connectionFactory = connectionFactory;
      this.tableManipulation = tableManipulation;
      this.marshaller = marshaller;
      this.timeService = timeService;
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
            log.tracef("Successfully removed %d rows.", result);
         }
      } catch (SQLException ex) {
         log.failedClearingJdbcCacheStore(ex);
         throw new CacheLoaderException("Failed clearing cache store", ex);
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
                  log.tracef("Executing batch %s, batch size is %d", readCount / batchSize, batchSize);
               }
            }
            objFromStream = marshaller.objectFromObjectStream(objectInput);
         }
         if (readCount % batchSize != 0) {
            ps.executeBatch();//flush the batch
         }
         if (log.isTraceEnabled()) {
            log.tracef("Successfully inserted %d buckets into the database, batch size is %d", readCount, batchSize);
         }
      } catch (IOException ex) {
         log.ioErrorIntegratingState(ex);
         throw new CacheLoaderException("I/O failure while integrating state into store", ex);
      } catch (SQLException e) {
         log.sqlFailureIntegratingState(e);
         throw new CacheLoaderException("SQL failure while integrating state into store", e);
      } catch (ClassNotFoundException e) {
         log.classNotFoundIntegratingState(e);
         throw new CacheLoaderException("Unexpected failure while integrating state into store", e);
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
            log.tracef("Running sql %s", sql);
         }
         connection = connectionFactory.getConnection();
         ps = connection.prepareStatement(sql);
         if (filterExpired) {
            ps.setLong(1, timeService.wallClockTime());
         }
         rs = ps.executeQuery();
         rs.setFetchSize(tableManipulation.getFetchSize());
         while (rs.next()) {
            InputStream is = rs.getBinaryStream(1);
            toStreamProcess(rs, is, objectOutput);
         }
         marshaller.objectToObjectStream(streamDelimiter, objectOutput);
      } catch (SQLException e) {
         log.sqlFailureStoringKeys(e);
         throw new CacheLoaderException("SQL Error while storing string keys to database", e);
      } catch (IOException e) {
         log.ioErrorStoringKeys(e);
         throw new CacheLoaderException("I/O Error while storing string keys to database", e);
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
            log.tracef("Running sql %s", sql);
         }
         conn = connectionFactory.getConnection();
         ps = conn.prepareStatement(sql);
         if (filterExpired) {
            ps.setLong(1, timeService.wallClockTime());
         }
         rs = ps.executeQuery();
         rs.setFetchSize(tableManipulation.getFetchSize());
         Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(tableManipulation.getFetchSize());
         while (rs.next()) {
            loadAllProcess(rs, result);
         }
         return result;
      } catch (SQLException e) {
         log.sqlFailureFetchingAllStoredEntries(e);
         throw new CacheLoaderException("SQL error while fetching all StoredEntries", e);
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
         log.sqlFailureFetchingAllStoredEntries(e);
         throw new CacheLoaderException("SQL error while fetching all StoredEntries", e);
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
            loadAllProcess(rs, result, maxEntries);
         }
         return result;
      } catch (SQLException e) {
         log.sqlFailureFetchingAllStoredEntries(e);
         throw new CacheLoaderException("SQL error while fetching all StoredEntries", e);
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

   protected abstract void loadAllProcess(ResultSet rs, Set<InternalCacheEntry> result, int maxEntries) throws SQLException, CacheLoaderException;

   protected abstract void loadAllKeysProcess(ResultSet rs, Set<Object> keys, Set<Object> keysToExclude) throws SQLException, CacheLoaderException;

   protected abstract void toStreamProcess(ResultSet rs, InputStream is, ObjectOutput objectOutput) throws CacheLoaderException, SQLException, IOException;

   protected abstract boolean fromStreamProcess(Object objFromStream, PreparedStatement ps, ObjectInput objectInput)
         throws SQLException, CacheLoaderException, IOException, ClassNotFoundException, InterruptedException;

}
