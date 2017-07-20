package org.infinispan.persistence.jpa.configuration;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

/**
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices
@Namespaces({
   @Namespace(root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:9.2", root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:9.1", root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:9.0", root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:8.0", root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:7.2", root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:7.1", root = "jpa-store"),
   @Namespace(uri = "urn:infinispan:config:store:jpa:7.0", root = "jpa-store"),
})
public class JpaStoreConfigurationParser implements ConfigurationParser {
   @Override
   public void readElement(XMLExtendedStreamReader reader,
         ConfigurationBuilderHolder holder) throws XMLStreamException {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case JPA_STORE: {
            ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
            parseJpaCacheStore(reader, builder.persistence().addStore(JpaStoreConfigurationBuilder.class), holder.getClassLoader());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseJpaCacheStore(XMLExtendedStreamReader reader, JpaStoreConfigurationBuilder builder, ClassLoader classLoader)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = StringPropertyReplacer.replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));

         switch (attribute) {
            case ENTITY_CLASS_NAME: {
               Class<?> clazz = Util.loadClass(value, classLoader);
               builder.entityClass(clazz);
               break;
            }
            case BATCH_SIZE: {
               builder.batchSize(Long.valueOf(value));
               break;
            }
            case PERSISTENCE_UNIT_NAME: {
               builder.persistenceUnitName(value);
               break;
            }
            case STORE_METADATA: {
               builder.storeMetadata(Boolean.valueOf(value));
               break;
            }
            default: {
               Parser.parseStoreAttribute(reader, i, builder);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Parser.parseStoreElement(reader, builder);
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
