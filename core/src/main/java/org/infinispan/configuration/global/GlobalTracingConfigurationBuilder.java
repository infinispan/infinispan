package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalTracingConfiguration.COLLECTOR_ENDPOINT;
import static org.infinispan.configuration.global.GlobalTracingConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalTracingConfiguration.EXPORTER_PROTOCOL;
import static org.infinispan.configuration.global.GlobalTracingConfiguration.SERVICE_NAME;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.util.logging.Log;

public class GlobalTracingConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalTracingConfiguration> {

   private final AttributeSet attributes;

   public GlobalTracingConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.attributes = GlobalTracingConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public GlobalTracingConfigurationBuilder collectorEndpoint(String collectorEndpoint) {
      attributes.attribute(COLLECTOR_ENDPOINT).set(collectorEndpoint);
      return this;
   }

   public boolean enabled() {
      return attributes.attribute(COLLECTOR_ENDPOINT).get() != null && attributes.attribute(ENABLED).get();
   }

   public GlobalTracingConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public GlobalTracingConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public GlobalTracingConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public GlobalTracingConfigurationBuilder exporterProtocol(TracingExporterProtocol exporterProtocol) {
      attributes.attribute(EXPORTER_PROTOCOL).set(exporterProtocol);
      return this;
   }

   public GlobalTracingConfigurationBuilder serviceName(String serviceName) {
      attributes.attribute(SERVICE_NAME).set(serviceName);
      return this;
   }

   @Override
   public GlobalTracingConfiguration create() {
      return new GlobalTracingConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(GlobalTracingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "GlobalTracingConfigurationBuilder [attributes=" + attributes + "]";
   }

   @Override
   public void validate() {
      String collectorEndpoint = attributes.attribute(COLLECTOR_ENDPOINT).get();
      if (collectorEndpoint == null) {
         return;
      }

      try {
         new URL(collectorEndpoint).toURI();
      } catch (MalformedURLException | URISyntaxException e) {
         throw Log.CONFIG.invalidTracingCollectorEndpoint(collectorEndpoint, e);
      }
   }
}
