package org.infinispan.tools.config.v6.jpa;


import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.tools.config.v6.Parser60;
import org.kohsuke.MetaInfServices;

/**
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@MetaInfServices
@Namespaces({@Namespace(uri = "urn:infinispan:config:jpa:6.0", root = "jpaStore")})
public class JpaStoreConfigurationParser60 implements ConfigurationParser {
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
         String value = reader.getAttributeValue(i);
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
               Parser60.parseCommonStoreAttributes(reader, builder, reader.getAttributeLocalName(i), value, i);
            }
         }
      }

      if (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         ParseUtils.unexpectedElement(reader);
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
