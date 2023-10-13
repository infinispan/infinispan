package org.infinispan.configuration.global;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configuration for tracing. See {@link GlobalTracingConfigurationBuilder}.
 */
public class GlobalTracingConfiguration {

   public static final AttributeDefinition<String> COLLECTOR_ENDPOINT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.COLLECTOR_ENDPOINT, null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, true, Boolean.class).immutable().build();
   public static final AttributeDefinition<TracingExporterProtocol> EXPORTER_PROTOCOL = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.EXPORTER_PROTOCOL, TracingExporterProtocol.OTLP).immutable().build();
   public static final AttributeDefinition<String> SERVICE_NAME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SERVICE_NAME, "infinispan-server").immutable().build();
   public static final AttributeDefinition<Boolean> SECURITY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SECURITY, false, Boolean.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalTracingConfiguration.class, COLLECTOR_ENDPOINT, ENABLED, EXPORTER_PROTOCOL, SERVICE_NAME, SECURITY);
   }

   private final AttributeSet attributes;

   GlobalTracingConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Enables tracing collection defining the collector that will receive the spans created by Infinispan.
    * The value is supposed to be a valid parsable URL containing the protocol,
    * the address and the port of the remote receiving process.
    * E.g., http://otlp-collector-host:4317.
    *
    * @return The collector endpoint URL.
    */
   public String collectorEndpoint() {
      return attributes.attribute(COLLECTOR_ENDPOINT).get();
   }

   /**
    * Tracing is enabled by default if "collector-endpoint" is defined.
    *
    * @return Whether the tracing is enabled
    */
   public boolean enabled() {
      return collectorEndpoint() != null && attributes.attribute(ENABLED).get();
   }

   /**
    * By default, the tracing spans will be exported applying the OTLP (OpenTelemetry Protocol).
    * This protocol can be changed, but an extra exporter dependency should be added in case.
    *
    * @return The protocol used to export the spans data.
    */
   public TracingExporterProtocol exporterProtocol() {
      return attributes.attribute(EXPORTER_PROTOCOL).get();
   }

   /**
    * The service name used by tracing to identify the server process.
    *
    * @return The service name.
    */
   public String serviceName() {
      return attributes.attribute(SERVICE_NAME).get();
   }

   /**
    * @return true whether the tracing of security events is enabled
    */
   public boolean security() {
      return enabled() && attributes.attribute(SECURITY).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GlobalTracingConfiguration that = (GlobalTracingConfiguration) o;
      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "GlobalTracingConfiguration{attributes=" + attributes + '}';
   }
}
