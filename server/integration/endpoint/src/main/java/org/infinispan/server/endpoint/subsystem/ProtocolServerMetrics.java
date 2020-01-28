package org.infinispan.server.endpoint.subsystem;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.dmr.ModelType;

public enum ProtocolServerMetrics {
   BYTES_READ(new SimpleAttributeDefinitionBuilder(MetricKeys.BYTES_READ, ModelType.LONG, true).setStorageRuntime().build()),
   BYTES_WRITTEN(new SimpleAttributeDefinitionBuilder(MetricKeys.BYTES_WRITTEN, ModelType.LONG, true).setStorageRuntime().build()),
   TRANSPORT_RUNNING(new SimpleAttributeDefinitionBuilder(MetricKeys.TRANSPORT_RUNNING, ModelType.BOOLEAN, true).setStorageRuntime().build());

   private static final Map<String, ProtocolServerMetrics> MAP = new HashMap<>();

   static {
      for (ProtocolServerMetrics stat : EnumSet.allOf(ProtocolServerMetrics.class)) {
         MAP.put(stat.toString(), stat);
      }
   }

   final AttributeDefinition definition;

   ProtocolServerMetrics(final AttributeDefinition definition) {
      this.definition = definition;
   }

   @Override
   public final String toString() {
      return definition.getName();
   }

   public static ProtocolServerMetrics getStat(final String stringForm) {
      return MAP.get(stringForm);
   }
}
