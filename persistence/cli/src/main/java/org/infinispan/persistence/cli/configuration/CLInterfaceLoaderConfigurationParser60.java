package org.infinispan.persistence.cli.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser60;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 * XML parser for CLI cache loader configuration.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Namespaces({
      @Namespace(uri = "urn:infinispan:config:cli:6.0", root = "cliLoader"),
      @Namespace(root = "cliLoader"),
})
public class CLInterfaceLoaderConfigurationParser60 implements ConfigurationParser {

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case CLI_LOADER: {
            parseCliLoader(reader, builder.persistence(), holder.getClassLoader());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseCliLoader(XMLExtendedStreamReader reader,
         PersistenceConfigurationBuilder persistence, ClassLoader classLoader)
         throws XMLStreamException {
      CLInterfaceLoaderConfigurationBuilder builder = new CLInterfaceLoaderConfigurationBuilder(persistence);
      parseCliLoaderAttributes(reader, builder, classLoader);
      persistence.addStore(builder);
   }

   private void parseCliLoaderAttributes(XMLExtendedStreamReader reader,
         CLInterfaceLoaderConfigurationBuilder builder, ClassLoader classLoader)
         throws XMLStreamException  {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);
         switch (attribute) {
            case CONNECTION: {
               builder.connectionString(value);
               break;
            }
            default: {
               Parser60.parseCommonStoreAttributes(reader, builder, attrName, value, i);
               break;
            }
         }
      }
   }

}
