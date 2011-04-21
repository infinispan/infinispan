/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

/**
 * An abstract configuration for JDBC cache stores which have support for locking.
 *
 * @author Manik Surtani
 * @version 4.1
 */
public abstract class AbstractNonDelegatingJdbcCacheStoreConfig extends AbstractJdbcCacheStoreConfig {
   private static final long serialVersionUID = 842757200078048889L;

   public static final int DEFAULT_CONCURRENCY_LEVEL = 2048;
   public static final int DEFAULT_LOCK_ACQUISITION_TIMEOUT = 60000;

   private int lockConcurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
   private long lockAcquistionTimeout = DEFAULT_LOCK_ACQUISITION_TIMEOUT;

   protected TableManipulation tableManipulation = new TableManipulation();
   protected boolean manageConnectionFactory = true;

   /**
    * Sets the name of the table where data will be stored.
    */
   public void setCacheName(String cacheName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setCacheName(cacheName);
   }

   /**
    * Sets the name of the column where the id will be stored. The id is obtained through:
    * <pre>
    *   key2StringMapper.getStringMapping(storedEntry.getKey());
    * </pre>
    * Mandatory.
    */
   public void setIdColumnName(String idColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setIdColumnName(idColumnName);
   }

   /**
    * Sets the name of the column where the StoredEntry will be binary stored. Mandatory.
    */
   public void setDataColumnName(String dataColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDataColumnName(dataColumnName);
   }

   /**
    * Sets the name of the column where the timestamp (Long in java) will be stored. Mandatory.
    */
   public void setTimestampColumnName(String timestampColumnName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTimestampColumnName(timestampColumnName);
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will be appended
    * to this prefix in order to enforce unique table names for each cache.
    */
   public void setTimestampColumnType(String timestampColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTimestampColumnType(timestampColumnType);
   }

   public TableManipulation getTableManipulation() {
      return tableManipulation;
   }

   /**
    * sql equivalent for java's String. Mandatory.
    */
   public void setIdColumnType(String idColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setIdColumnType(idColumnType);
   }

   /**
    * Sets the type of the column where data will be binary stored. It should be a binary type like <tt>BLOB</tt>.
    * Mandatory.
    * <br>
    * Character types like <tt>CLOB</tt>/<tt>LONGVARCHAR</tt>/<tt>VARCHAR</tt> are <b>not</b> supported.
    * <br>
    * Note that on MySQL you need to use <a href="http://dev.mysql.com/doc/refman/5.0/en/server-sql-mode.html#sqlmode_strict_all_tables">strict mode</a>
    * or the database will truncate long values without raising any errors, leading to unmarshalling errors on load.
    */
   public void setDataColumnType(String dataColumnType) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDataColumnType(dataColumnType);
   }

   public void setDropTableOnExit(boolean dropTableOnExit) {
      testImmutability("tableManipulation");
      this.tableManipulation.setDropTableOnExit(dropTableOnExit);
   }

   public void setCreateTableOnStart(boolean createTableOnStart) {
      testImmutability("tableManipulation");
      this.tableManipulation.setCreateTableOnStart(createTableOnStart);
   }

   /**
    * If this method returns false, then the connection factory should not be created by the {@link
    * org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}, but will be injected through {@link
    * org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore#doConnectionFactoryInitialization(org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory)}
    */
   public boolean isManageConnectionFactory() {
      return manageConnectionFactory;
   }

   public void setTableManipulation(TableManipulation tableManipulation) {
      testImmutability("tableManipulation");
      this.tableManipulation = tableManipulation;
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   public void setFetchSize(int fetchSize) {
      testImmutability("tableManipulation");
      this.tableManipulation.setFetchSize(fetchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   public void setBatchSize(int batchSize) {
      testImmutability("tableManipulation");
      this.tableManipulation.setBatchSize(batchSize);
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getFetchSize()
    */
   public int getFetchSize() {
      return this.tableManipulation.getFetchSize();
   }

   /**
    * @see org.infinispan.loaders.jdbc.TableManipulation#getBatchSize()
    */
   public int getBatchSize() {
      return this.tableManipulation.getBatchSize();
   }

   public String getDatabaseType() {
      return this.tableManipulation.databaseType == null ? "" : this.tableManipulation.databaseType.toString();
   }

   /**
    * Sets the database dialect.  Valid types are reflected in the DatabaseType enum.  If unspecified, will attempt to
    * "guess" appropriate dialect from the JDBC driver specified.
    *
    * @param dbType
    */
   public void setDatabaseType(String dbType) {
      if (dbType != null)
         this.tableManipulation.databaseType = DatabaseType.valueOf(dbType.toUpperCase().trim());
   }


   @Override
   public AbstractNonDelegatingJdbcCacheStoreConfig clone() {
      AbstractNonDelegatingJdbcCacheStoreConfig result = (AbstractNonDelegatingJdbcCacheStoreConfig) super.clone();
      result.tableManipulation = tableManipulation.clone();
      return result;
   }

   @Override
   public String toString() {
      return "AbstractNonDelegatingJdbcCacheStoreConfig{" +
            "lockConcurrencyLevel=" + lockConcurrencyLevel +
            ", lockAcquistionTimeout=" + lockAcquistionTimeout +
            ", tableManipulation=" + tableManipulation +
            ", manageConnectionFactory=" + manageConnectionFactory +
            "} " + super.toString();
   }
}
