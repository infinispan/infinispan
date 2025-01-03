package org.infinispan.cdc.internal.configuration;

import java.util.Collection;

public record Table(String name, PrimaryKey primaryKey, Collection<ForeignKey> foreignKeys, Collection<String> columns) { }
