package org.infinispan.persistence.jdbc.table.management;

import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class SQLServerTableManager extends AbstractTableManager {

   private static final Log LOG = LogFactory.getLog(MySQLTableManager.class, Log.class);

   SQLServerTableManager(ConnectionFactory connectionFactory, TableManipulationConfiguration config, DbMetaData metaData) {
      super(connectionFactory, config, metaData, LOG);
   }

   @Override
   public boolean isStringEncodingRequired() {
      return metaData.getMajorVersion() < 13;
   }

   @Override
   public String encodeString(String string) {
      char[] srcChars = string.toCharArray();
      if (srcChars.length > 0 && srcChars[0] == '\uFEFF') {
         char[] chars = new char[srcChars.length - 1];
         string.getChars(1, string.toCharArray().length, chars, 0);
         return new String(chars);
      }
      return string;
   }
}
