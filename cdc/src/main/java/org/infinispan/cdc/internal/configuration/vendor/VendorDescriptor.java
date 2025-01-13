package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

/**
 * @since 15.2
 */
public interface VendorDescriptor {

   PrimaryKey primaryKey(Connection connection, String table) throws SQLException;

   List<ForeignKey> foreignKeys(Connection connection, String table) throws SQLException;
}
