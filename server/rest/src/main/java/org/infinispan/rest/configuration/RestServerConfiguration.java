package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

import io.netty.handler.codec.http.cors.CorsConfig;

@BuiltBy(RestServerConfigurationBuilder.class)
@ConfigurationFor(RestServer.class)
public class RestServerConfiguration extends ProtocolServerConfiguration {
   public static final AttributeDefinition<ExtendedHeaders> EXTENDED_HEADERS = AttributeDefinition.builder("extended-header", ExtendedHeaders.ON_DEMAND).immutable().build();
   public static final AttributeDefinition<String> CONTEXT_PATH = AttributeDefinition.builder("context-path", "rest").immutable().build();
   public static final AttributeDefinition<Integer> MAX_CONTENT_LENGTH = AttributeDefinition.builder("max-content-length", 10 * 1024 * 1024).immutable().build();
   public static final AttributeDefinition<List<CorsConfig>> CORS_RULES = AttributeDefinition.builder("cors-rules", null, (Class<List<CorsConfig>>) (Class<?>) List.class)
         .initializer(() -> new ArrayList<>(3)).immutable().build();
   public static final AttributeDefinition<Integer> COMPRESSION_LEVEL = AttributeDefinition.builder("compression-level", 6).immutable().build();

   private final Attribute<ExtendedHeaders> extendedHeaders;
   private final Attribute<String> contextPath;
   private final Attribute<Integer> maxContentLength;
   private final Attribute<List<CorsConfig>> corsRules;
   private final Attribute<Integer> compressionLevel;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RestServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(),
            EXTENDED_HEADERS, CONTEXT_PATH, MAX_CONTENT_LENGTH, CORS_RULES, COMPRESSION_LEVEL);
   }

   RestServerConfiguration(AttributeSet attributes, SslConfiguration ssl) {
      super(attributes, ssl);
      this.extendedHeaders = attributes.attribute(EXTENDED_HEADERS);
      this.contextPath = attributes.attribute(CONTEXT_PATH);
      this.maxContentLength = attributes.attribute(MAX_CONTENT_LENGTH);
      this.corsRules = attributes.attribute(CORS_RULES);
      this.compressionLevel = attributes.attribute(COMPRESSION_LEVEL);
   }

   public ExtendedHeaders extendedHeaders() {
      return extendedHeaders.get();
   }

   /**
    * @deprecated Use {@link #ignoredCaches()} instead.
    */
   @Deprecated
   public Set<String> getIgnoredCaches() {
      return ignoredCaches();
   }

   public String contextPath() {
      return contextPath.get();
   }

   public int maxContentLength() {
      return maxContentLength.get();
   }

   public List<CorsConfig> getCorsRules() {
      return corsRules.get();
   }

   public int getCompressionLevel() {
      return compressionLevel.get();
   }

   @Override
   public String toString() {
      return "RestServerConfiguration[" + attributes + ", ssl=" + ssl + "]";
   }
}
