package org.infinispan.cdc.internal.configuration;

import java.util.List;

public record PrimaryKey(String name, List<String> columns) { }
