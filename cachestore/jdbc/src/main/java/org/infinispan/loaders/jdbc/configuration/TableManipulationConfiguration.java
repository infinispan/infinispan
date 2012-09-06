/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jdbc.configuration;

public class TableManipulationConfiguration {
   private final String idColumnName;
   private final String idColumnType;
   private final String tableNamePrefix;
   private final String cacheName;
   private final String dataColumnName;
   private final String dataColumnType;
   private final String timestampColumnName;
   private final String timestampColumnType;
   private final int fetchSize;
   private final int batchSize;
   private final boolean createOnStart;
   private final boolean dropOnExit;

   TableManipulationConfiguration(String idColumnName, String idColumnType, String tableNamePrefix, String cacheName,
         String dataColumnName, String dataColumnType, String timestampColumnName, String timestampColumnType,
         int fetchSize, int batchSize, boolean createOnStart, boolean dropOnExit) {
      this.idColumnName = idColumnName;
      this.idColumnType = idColumnType;
      this.tableNamePrefix = tableNamePrefix;
      this.cacheName = cacheName;
      this.dataColumnName = dataColumnName;
      this.dataColumnType = dataColumnType;
      this.timestampColumnName = timestampColumnName;
      this.timestampColumnType = timestampColumnType;
      this.batchSize = batchSize;
      this.fetchSize = fetchSize;
      this.createOnStart = createOnStart;
      this.dropOnExit = dropOnExit;
   }

   public boolean createOnStart() {
      return createOnStart;
   }

   public boolean dropOnExit() {
      return dropOnExit;
   }

   public String idColumnName() {
      return idColumnName;
   }

   public String idColumnType() {
      return idColumnType;
   }

   public String tableNamePrefix() {
      return tableNamePrefix;
   }

   public String cacheName() {
      return cacheName;
   }

   public String dataColumnName() {
      return dataColumnName;
   }

   public String dataColumnType() {
      return dataColumnType;
   }

   public String timestampColumnName() {
      return timestampColumnName;
   }

   public String timestampColumnType() {
      return timestampColumnType;
   }

   public int fetchSize() {
      return fetchSize;
   }

   public int batchSize() {
      return batchSize;
   }

   @Override
   public String toString() {
      return "TableManipulationConfiguration [idColumnName=" + idColumnName + ", idColumnType=" + idColumnType
            + ", tableNamePrefix=" + tableNamePrefix + ", cacheName=" + cacheName + ", dataColumnName="
            + dataColumnName + ", dataColumnType=" + dataColumnType + ", timestampColumnName=" + timestampColumnName
            + ", timestampColumnType=" + timestampColumnType + ", fetchSize=" + fetchSize + ", batchSize=" + batchSize
            + "]";
   }
}