package org.infinispan.tools.store.migrator.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.AbstractTableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
abstract class AbstractJdbcEntryIterator implements Iterator<MarshallableEntry>, AutoCloseable {
   final ConnectionFactory connectionFactory;
   final TableManager tableManager;
   final Marshaller marshaller;
   private Connection conn;
   private PreparedStatement ps;
   ResultSet rs;
   long numberOfRows = 0;
   int rowIndex = 0;

   AbstractJdbcEntryIterator(ConnectionFactory connectionFactory, TableManager tableManager,
                             Marshaller marshaller) {
      this.connectionFactory = connectionFactory;
      this.tableManager = tableManager;
      this.marshaller = marshaller;

      try {
         conn = connectionFactory.getConnection();
         String sizeSql = ((AbstractTableManager<?, ?>) tableManager).getSizeSql();
         ps = conn.prepareStatement(sizeSql);
         ps.setLong(1, System.currentTimeMillis());
         rs = ps.executeQuery();
         rs.next();
         numberOfRows = rs.getInt(1);
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(ps);

         ps = conn.prepareStatement(tableManager.getLoadAllRowsSql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
         ps.setFetchSize(tableManager.getFetchSize());
         rs = ps.executeQuery();
      } catch (SQLException e) {
         this.close();
         throw new PersistenceException("SQL error while fetching all StoredEntries", e);
      }
   }

   @Override
   public void close() {
      JdbcUtil.safeClose(rs);
      JdbcUtil.safeClose(ps);
      connectionFactory.releaseConnection(conn);
   }
}
