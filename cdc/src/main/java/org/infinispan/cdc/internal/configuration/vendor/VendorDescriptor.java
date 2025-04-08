package org.infinispan.cdc.internal.configuration.vendor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.infinispan.cdc.internal.configuration.ForeignKey;
import org.infinispan.cdc.internal.configuration.PrimaryKey;

/**
 * Vendor specific representation of the database.
 *
 * <p>
 * The descriptor abstracts implementation details of a given database vendor. For example, how to perform a statement,
 * or how to retrieve table constraints.
 * </p>
 *
 * @since 16.0
 * @author Tristan Tarrant
 */
public interface VendorDescriptor {

   /**
    * Retrieve the primary key constraint of a table.
    *
    * <p>
    * Utilizes the given connection to query the database and retrieve the primary key constraint of the requested
    * table.
    * </p>
    *
    * @param connection A connection established to the database.
    * @param table The table name to retrieve the constraint.
    * @return A description of the constraint.
    * @throws SQLException If failed to execute the queries.
    */
   PrimaryKey primaryKey(Connection connection, String table) throws SQLException;

   /**
    * Retrieve all foreign keys in a single table.
    *
    * <p>
    * Utilizes the given connection to query the database and retrieve all foreign keys contained in a table. The description
    * includes the local column and which is the referenced table and column.
    * </p>
    *
    * @param connection A connection established to the database.
    * @param table The table name to retrieve the constraints.
    * @return A list with all foreign keys.
    * @throws SQLException If failed to execute the queries.
    */
   List<ForeignKey> foreignKeys(Connection connection, String table) throws SQLException;
}
