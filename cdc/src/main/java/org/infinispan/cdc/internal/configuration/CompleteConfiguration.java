package org.infinispan.cdc.internal.configuration;

public record CompleteConfiguration(ConnectionParameters connection, Table table) { }
