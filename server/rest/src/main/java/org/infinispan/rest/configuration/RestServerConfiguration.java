package org.infinispan.rest.configuration;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.configuration.EncryptionConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

import io.netty.handler.codec.http.cors.CorsConfig;

@BuiltBy(RestServerConfigurationBuilder.class)
@ConfigurationFor(RestServer.class)
public class RestServerConfiguration extends ProtocolServerConfiguration {
   public static final AttributeDefinition<ExtendedHeaders> EXTENDED_HEADERS = AttributeDefinition.builder("extended-headers", ExtendedHeaders.ON_DEMAND).immutable().build();
   public static final AttributeDefinition<String> CONTEXT_PATH = AttributeDefinition.builder("context-path", "rest").immutable().build();
   public static final AttributeDefinition<Integer> MAX_CONTENT_LENGTH = AttributeDefinition.builder("max-content-length", 10 * 1024 * 1024).immutable().build();
   public static final AttributeDefinition<Integer> COMPRESSION_LEVEL = AttributeDefinition.builder("compression-level", 6).immutable().build();

   private final Attribute<ExtendedHeaders> extendedHeaders;
   private final Attribute<String> contextPath;
   private final Attribute<Integer> maxContentLength;
   private final Attribute<Integer> compressionLevel;
   private final Path staticResources;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RestServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(),
            WORKER_THREADS, EXTENDED_HEADERS, CONTEXT_PATH, MAX_CONTENT_LENGTH, COMPRESSION_LEVEL);
   }

   private final AuthenticationConfiguration authentication;
   private final CorsConfiguration cors;
   private final EncryptionConfiguration encryption;

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("rest-connector");

   private final List<ConfigurationInfo> elements;

   RestServerConfiguration(AttributeSet attributes, SslConfiguration ssl,
                           Path staticResources, AuthenticationConfiguration authentication,
                           CorsConfiguration cors,
                           EncryptionConfiguration encryption) {
      super(attributes, ssl);
      this.staticResources = staticResources;
      this.authentication = authentication;
      this.extendedHeaders = attributes.attribute(EXTENDED_HEADERS);
      this.contextPath = attributes.attribute(CONTEXT_PATH);
      this.maxContentLength = attributes.attribute(MAX_CONTENT_LENGTH);
      this.cors = cors;
      this.compressionLevel = attributes.attribute(COMPRESSION_LEVEL);
      this.encryption = encryption;
      this.elements = Arrays.asList(authentication, cors, encryption);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public EncryptionConfiguration encryption() {
      return encryption;
   }

   public ExtendedHeaders extendedHeaders() {
      return extendedHeaders.get();
   }

   public Path staticResources() {
      return staticResources;
   }

   public String contextPath() {
      return contextPath.get();
   }

   public int maxContentLength() {
      return maxContentLength.get();
   }

   public List<CorsConfig> getCorsRules() {
      return cors.corsConfigs();
   }

   public CorsConfiguration cors() {
      return cors;
   }

   public int getCompressionLevel() {
      return compressionLevel.get();
   }

   @Override
   public String toString() {
      return "RestServerConfiguration{" +
            "authentication=" + authentication +
            ", cors=" + cors +
            ", encryption=" + encryption +
            '}';
   }
}
