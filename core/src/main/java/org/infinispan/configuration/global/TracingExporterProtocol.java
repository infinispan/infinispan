package org.infinispan.configuration.global;

public enum TracingExporterProtocol {

   /**
    * Use the OpenTelemetry Protocol (OTLP). This is the default and the recommended option.
    * This option does not require any extra dependency.
    */
   OTLP,

   /**
    * Tracing will be exported using gRPC and the Jaeger format.
    * This option will require the Jaeger exporter as extra dependency.
    */
   JAEGER,

   /**
    * Tracing will be exported in JSON Zipkin format using HTTP.
    * This option will require the Zipkin exporter as extra dependency.
    */
   ZIPKIN,

   /**
    * Tracing will be exported for a Prometheus collector.
    * This option will require the Prometheus exporter as extra dependency.
    */
   PROMETHEUS;

}
