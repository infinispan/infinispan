package org.infinispan.cdc.internal.configuration;

import java.util.Collection;

/**
 * A single table in the database.
 *
 * @param name The table name.
 * @param primaryKey The primary key of the table.
 * @param foreignKeys The list of foreign keys of the table.
 * @param columns The remaining columns of the table.
 * @since 15.2
 */
public record Table(String name, PrimaryKey primaryKey, Collection<ForeignKey> foreignKeys, Collection<String> columns) { }
