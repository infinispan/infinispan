package org.infinispan.cdc.internal.configuration;

import java.util.List;

/**
 * A single foreign key constraint.
 *
 * @param name The constraint name in the database.
 * @param columns The list of columns included in the constraint in the source table.
 * @param refTable The name of the referenced table.
 * @param refColumns The list of columns included in the constraint in the referenced table.
 * @since 16.0
 */
public record ForeignKey(String name, List<String> columns, String refTable, List<String> refColumns) { }
