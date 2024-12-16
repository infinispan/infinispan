package org.infinispan.cdc.internal.configuration;

import java.util.List;

public record ForeignKey(String name, List<String> columns, String refTable, List<String> refColumns) { }
