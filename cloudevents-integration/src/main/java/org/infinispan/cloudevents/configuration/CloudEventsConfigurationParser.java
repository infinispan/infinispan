package org.infinispan.cloudevents.configuration;


import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.NAMESPACE;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

/**
 * CloudEvents integration parser extension.
 *
 * This extension parses elements in the "urn:infinispan:config:ce" namespace
 *
 * @author Dan Berindei
 * @since 12
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "cloudevents"),
      @Namespace(root = "cloudevents-cache"),
      @Namespace(uri = NAMESPACE + "*", root = "cloudevents", since = "12.0"),
      @Namespace(uri = NAMESPACE + "*", root = "cloudevents-cache", since = "12.0"),
})
public class CloudEventsConfigurationParser implements ConfigurationParser {

   static final String PREFIX = "cloudevents";
   static final String NAMESPACE = Parser.NAMESPACE + PREFIX + ":";

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      if (holder.inScope(ParserScope.CACHE_CONTAINER)) {
         GlobalConfigurationBuilder globalBuilder = holder.getGlobalConfigurationBuilder();
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLOUDEVENTS: {
               CloudEventsGlobalConfigurationBuilder cloudEventsGlobalBuilder =
                     globalBuilder.addModule(CloudEventsGlobalConfigurationBuilder.class);
               parseGlobalCloudEvents(reader, cloudEventsGlobalBuilder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      } else if (holder.inScope(ParserScope.CACHE) || holder.inScope(ParserScope.CACHE_TEMPLATE)) {
         ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLOUDEVENTS_CACHE: {
               CloudEventsConfigurationBuilder cloudEventsBuilder =
                     builder.addModule(CloudEventsConfigurationBuilder.class);
               parseCacheCloudEvents(reader, cloudEventsBuilder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      } else {
         throw new IllegalStateException("WRONG SCOPE");
      }
   }

   private void parseGlobalCloudEvents(XMLExtendedStreamReader reader, CloudEventsGlobalConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case BOOTSTRAP_SERVERS: {
               builder.bootstrapServers(value);
               break;
            }
            case ACKS: {
               builder.acks(value);
               break;
            }
            case AUDIT_TOPIC: {
               builder.auditTopic(value);
               break;
            }
            case CACHE_ENTRIES_TOPIC: {
               builder.cacheEntriesTopic(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseCacheCloudEvents(XMLExtendedStreamReader reader, CloudEventsConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED: {
               builder.enabled(Boolean.parseBoolean(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
