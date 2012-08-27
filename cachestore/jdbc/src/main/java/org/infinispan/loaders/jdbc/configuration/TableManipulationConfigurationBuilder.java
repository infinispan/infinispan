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

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;
import org.infinispan.loaders.jdbc.TableManipulation;

/**
 * TableManipulationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class TableManipulationConfigurationBuilder extends
      AbstractStoreConfigurationChildBuilder<TableManipulationConfiguration> {

   private int batchSize = TableManipulation.DEFAULT_BATCH_SIZE;
   private int fetchSize = TableManipulation.DEFAULT_FETCH_SIZE;
   private boolean createOnStart = true;
   private boolean dropOnExit = false;
   private String tableNamePrefix;
   private String cacheName;
   private String idColumnName;
   private String idColumnType;
   private String dataColumnName;
   private String dataColumnType;
   private String timestampColumnName;
   private String timestampColumnType;

   TableManipulationConfigurationBuilder(
         AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder);
   }

   public TableManipulationConfigurationBuilder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
   }

   public TableManipulationConfigurationBuilder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
   }

   public TableManipulationConfigurationBuilder tableNamePrefix(String tableNamePrefix) {
      this.tableNamePrefix = tableNamePrefix;
      return this;
   }

   public TableManipulationConfigurationBuilder createOnStart(boolean createOnStart) {
      this.createOnStart = createOnStart;
      return this;
   }

   public TableManipulationConfigurationBuilder dropOnExit(boolean dropOnExit) {
      this.dropOnExit = dropOnExit;
      return this;
   }

   public TableManipulationConfigurationBuilder cacheName(String cacheName) {
      this.cacheName = cacheName;
      return this;
   }

   public TableManipulationConfigurationBuilder idColumnName(String idColumnName) {
      this.idColumnName = idColumnName;
      return this;
   }

   public TableManipulationConfigurationBuilder idColumnType(String idColumnType) {
      this.idColumnType = idColumnType;
      return this;
   }

   public TableManipulationConfigurationBuilder dataColumnName(String dataColumnName) {
      this.dataColumnName = dataColumnName;
      return this;
   }

   public TableManipulationConfigurationBuilder dataColumnType(String dataColumnType) {
      this.dataColumnType = dataColumnType;
      return this;
   }

   public TableManipulationConfigurationBuilder timestampColumnName(String timestampColumnName) {
      this.timestampColumnName = timestampColumnName;
      return this;
   }

   public TableManipulationConfigurationBuilder timestampColumnType(String timestampColumnType) {
      this.timestampColumnType = timestampColumnType;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public TableManipulationConfiguration create() {
      return new TableManipulationConfiguration(idColumnName, idColumnType, tableNamePrefix, cacheName, dataColumnName,
            dataColumnType, timestampColumnName, timestampColumnType, fetchSize, batchSize, createOnStart, dropOnExit);
   }

   @Override
   public Builder<?> read(TableManipulationConfiguration template) {
      this.batchSize = template.batchSize();
      this.fetchSize = template.fetchSize();
      this.createOnStart = template.createOnStart();
      this.dropOnExit = template.dropOnExit();
      this.idColumnName = template.idColumnName();
      this.idColumnType = template.idColumnType();
      this.dataColumnName = template.dataColumnName();
      this.dataColumnType = template.dataColumnType();
      this.timestampColumnName = template.timestampColumnName();
      this.timestampColumnType = template.timestampColumnType();
      this.cacheName = template.cacheName();
      this.tableNamePrefix = template.tableNamePrefix();

      return this;
   }



}