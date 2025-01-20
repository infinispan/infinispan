package org.infinispan.cdc.internal.configuration.vendor;

public enum DatabaseVendor {
   DB2 {
      @Override
      public VendorDescriptor descriptor() {
         return new DB2Descriptor();
      }
   },
   MYSQL {
      @Override
      public VendorDescriptor descriptor() {
         return new MySQLDescriptor();
      }
   },
   ORACLE {
      @Override
      public VendorDescriptor descriptor() {
         return new OracleDescriptor();
      }
   },
   POSTGRES {
      @Override
      public VendorDescriptor descriptor() {
         return new PostgresDescriptor();
      }
   },
   SQL_SERVER {
      @Override
      public VendorDescriptor descriptor() {
         return new MSSQLDescriptor();
      }
   },
   ;

   public abstract VendorDescriptor descriptor();
}
