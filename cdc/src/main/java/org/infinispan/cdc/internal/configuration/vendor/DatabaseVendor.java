package org.infinispan.cdc.internal.configuration.vendor;

/**
 * Accepted database vendors for change data capture.
 *
 * @since 16.0
 * @author José Bolina
 */
public enum DatabaseVendor {
   DB2 {
      @Override
      public VendorDescriptor descriptor() {
         return new DB2Descriptor();
      }
   },
   MSSQL {
      @Override
      public VendorDescriptor descriptor() {
         return new MSSQLDescriptor();
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
   ;

   /**
    * The vendor specific descriptor.
    *
    * @return The descriptor implementation for the vendor.
    */
   public abstract VendorDescriptor descriptor();
}
