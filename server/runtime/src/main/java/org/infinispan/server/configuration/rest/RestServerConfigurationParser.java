package org.infinispan.server.configuration.rest;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.core.configuration.SslConfigurationBuilder;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

/**
 * Server endpoint configuration parser
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "rest-connector"),
      @Namespace(uri = "urn:infinispan:server:*", root = "rest-connector"),
})
public class RestServerConfigurationParser implements ConfigurationParser {
   private static org.infinispan.util.logging.Log coreLog = LogFactory.getLog(ServerConfigurationParser.class);

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      if (!holder.inScope(ServerConfigurationParser.ENDPOINTS_SCOPE)) {
         throw coreLog.invalidScope(ServerConfigurationParser.ENDPOINTS_SCOPE, holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case REST_CONNECTOR: {
            ServerConfigurationBuilder serverBuilder = builder.module(ServerConfigurationBuilder.class);
            if (serverBuilder != null) {
               parseRest(reader, serverBuilder);
            } else {
               throw ParseUtils.unexpectedElement(reader);
            }
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private void parseRest(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder)
         throws XMLStreamException {
      RestServerConfigurationBuilder builder = serverBuilder.addConnector(RestServerConfigurationBuilder.class);
      boolean hasSocketBinding = false;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CONTEXT_PATH: {
               builder.contextPath(value);
               break;
            }
            case EXTENDED_HEADERS: {
               builder.extendedHeaders(ExtendedHeaders.valueOf(value));
               break;
            }
            case NAME: {
               builder.name(value);
               break;
            }
            case MAX_CONTENT_LENGTH: {
               builder.maxContentLength(Integer.parseInt(value));
               break;
            }
            case COMPRESSION_LEVEL: {
               builder.compressionLevel(Integer.parseInt(value));
               break;
            }
            case IGNORED_CACHES: {
               Set<String> ignoredCaches = new HashSet<>();
               String[] values = reader.getListAttributeValue(i);
               for (String v : values) {
                  ignoredCaches.add(v);
               }
               builder.ignoredCaches(ignoredCaches);
               break;
            }
            case SOCKET_BINDING: {
               serverBuilder.applySocketBinding(value, builder);
               hasSocketBinding = true;
               break;
            }
            default: {
               ServerConfigurationParser.parseCommonConnectorAttributes(reader, i, serverBuilder, builder);
            }
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case AUTHENTICATION: {
               parseAuthentication(reader, builder);
               break;
            }
            case ENCRYPTION: {
               parseEncryption(reader, serverBuilder, builder.ssl().enable());
               break;
            }
            case CORS_RULES: {
               parseCorsRules(reader, builder);
               break;
            }
            default:
               throw ParseUtils.unexpectedElement(reader);
         }

      }
      if (!hasSocketBinding) {
         // This connector will be part of the single port router
         builder.startTransport(false);
      }
   }

   private void parseCorsRules(XMLExtendedStreamReader reader, RestServerConfigurationBuilder builder)
         throws XMLStreamException {
      ParseUtils.requireNoAttributes(reader);
      List<CorsConfig> rules = new ArrayList<>();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CORS_RULE: {
               rules.add(parseCorsRule(reader));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      builder.addAll(rules);
   }

   private CorsConfig parseCorsRule(XMLExtendedStreamReader reader) throws XMLStreamException {
      boolean allowCredentials = false;
      Optional<Long> maxAge = Optional.empty();
      Optional<String[]> allowedHeaders = Optional.empty();
      Optional<String[]> allowedOrigins = Optional.empty();
      Optional<HttpMethod[]> allowedMethods = Optional.empty();
      Optional<String[]> exposeHeaders = Optional.empty();

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ALLOW_CREDENTIALS: {
               allowCredentials = true;
               break;
            }
            case MAX_AGE_SECONDS: {
               maxAge = Optional.of(Long.parseLong(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ALLOWED_HEADERS: {
               allowedHeaders = Optional.of(reader.getElementText().split(","));
               break;
            }
            case ALLOWED_ORIGINS: {
               allowedOrigins = Optional.of(reader.getElementText().split(","));
               break;
            }
            case ALLOWED_METHODS: {
               allowedMethods = Optional.of(Arrays.stream(reader.getElementText().split(",")).map(HttpMethod::valueOf).toArray(HttpMethod[]::new));
               break;
            }
            case EXPOSE_HEADERS: {
               exposeHeaders = Optional.of(reader.getElementText().split(","));
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      CorsConfigBuilder builder = allowedOrigins.isPresent() ? CorsConfigBuilder.forOrigins(allowedOrigins.get()) : CorsConfigBuilder.forAnyOrigin();
      if (allowCredentials) builder.allowCredentials();
      maxAge.ifPresent(a -> builder.maxAge(a));
      allowedHeaders.ifPresent(h -> builder.allowedRequestHeaders(h));
      allowedMethods.ifPresent(m -> builder.allowedRequestMethods(m));
      exposeHeaders.ifPresent(h -> builder.exposeHeaders(h));
      return builder.build();
   }

   private void parseAuthentication(XMLExtendedStreamReader reader, RestServerConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SECURITY_REALM: {
               break;
            }
            case AUTH_METHOD: {
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseEncryption(XMLExtendedStreamReader reader, ServerConfigurationBuilder serverBuilder, SslConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         String value = replaceProperties(reader.getAttributeValue(i));
         switch (attribute) {
            case REQUIRE_SSL_CLIENT_AUTH: {
               builder.requireClientAuth(Boolean.parseBoolean(value));
               break;
            }
            case SECURITY_REALM: {
               builder.sslContext(serverBuilder.getSSLContext(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }

      //Since nextTag() moves the pointer, we need to make sure we won't move too far
      boolean skipTagCheckAtTheEnd = reader.hasNext();

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case SNI: {
               parseSni(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }

      if (!skipTagCheckAtTheEnd)
         ParseUtils.requireNoContent(reader);
   }

   private void parseSni(XMLExtendedStreamReader reader, SslConfigurationBuilder builder) {

   }

}
