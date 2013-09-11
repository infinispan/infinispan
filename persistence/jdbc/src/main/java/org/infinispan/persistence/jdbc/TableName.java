package org.infinispan.persistence.jdbc;

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
