package org.infinispan.cdc.internal.configuration;

import java.util.List;

/**
 * Represent a primary key constraint in the database.
 *
 * @param name The name of the constraint in the database.
 * @param columns The columns included in the constraint.
 * @since 16.0
 */
public record PrimaryKey(String name, List<String> columns) { }
