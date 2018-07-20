package org.infinispan.tools.store.migrator.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.table.management.TableManager;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
abstract class AbstractJdbcEntryIterator implements Iterator<MarshalledEntry>, AutoCloseable {
   final ConnectionFactory connectionFactory;
   final TableManager tableManager;
   final Marshaller marshaller;
   private Connection conn;
   private PreparedStatement ps;
   ResultSet rs;
   int numberOfRows = 0;
   int rowIndex = 0;

   AbstractJdbcEntryIterator(ConnectionFactory connectionFactory, TableManager tableManager,
                             Marshaller marshaller) {
      this.connectionFactory = connectionFactory;
      this.tableManager = tableManager;
      this.marshaller = marshaller;

      PreparedStatement countPs = null;
      ResultSet countRs = null;
      try {
         conn = connectionFactory.getConnection();

         countPs = conn.prepareStatement(tableManager.getCountNonExpiredRowsSql());
         countPs.setLong(1, System.currentTimeMillis());
         countRs = countPs.executeQuery();
         countRs.next();
         numberOfRows = countRs.getInt(1);

         ps = conn.prepareStatement(tableManager.getLoadAllRowsSql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
         ps.setFetchSize(tableManager.getFetchSize());
         rs = ps.executeQuery();
      } catch (SQLException e) {
         throw new PersistenceException("SQL error while fetching all StoredEntries", e);
      } finally {
         JdbcUtil.safeClose(countPs);
         JdbcUtil.safeClose(countRs);
      }
   }

   @Override
   public void close() {
      JdbcUtil.safeClose(rs);
      JdbcUtil.safeClose(ps);
      connectionFactory.releaseConnection(conn);
   }
}
