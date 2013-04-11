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

import java.io.Serializable;

/**
* Value object for table name operations.
*/
public class TableName implements Serializable {

   private String identifierQuote;
   private String schema;
   private String tableName;

   public TableName(String identifierQuote, String tableNamePrefix, String cacheName){
      if(identifierQuote == null){
         throw new IllegalArgumentException("identifierQuote must not be null");
      }
      if(tableNamePrefix == null){
         throw new IllegalArgumentException("tableNamePrefix must not be null");
      }
      if(cacheName == null){
         throw new IllegalArgumentException("cacheName must not be null");
      }
      this.identifierQuote = identifierQuote;
      normalize(tableNamePrefix, cacheName);

   }

   public String getSchema(){
      return schema;
   }

   public String getName(){
      return tableName;
   }

   /**
    *
    * @return full qualified table name (contains schema and name) in a quoted way.
    */
   @Override
   public String toString() {
      if(schema != null){
         return identifierQuote + schema + identifierQuote + "." + identifierQuote + tableName + identifierQuote;
      } else {
         return identifierQuote + tableName + identifierQuote;
      }
   }

   private void normalize(String tableNamePrefix, String cacheName){
      cacheName = cacheName.replaceAll("[^\\p{Alnum}]", "_");
      String tableName = (tableNamePrefix + "_" + cacheName);
      // split table name to determine optional used schema
      String[] tableNameParts = tableName.split("\\.", 2);
      if(tableNameParts.length != 1){
         this.schema = tableNameParts[0];
         this.tableName = tableNameParts[1];
      } else {
         this.schema = null;
         this.tableName = tableNameParts[0];
      }
      if(schema != null && schema.isEmpty()){
         throw new IllegalArgumentException("Schema inside table name prefix must not be empty.");
      }
   }


}
