package org.infinispan.persistence.cli.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser72;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 * XML parser for CLI cache loader configuration.
 *
 * @author Galder Zamarreño
 * @since 7.1
 */
@MetaInfServices
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:store:cli:7.0", root = "cli-loader"),
   @Namespace(uri = "urn:infinispan:config:store:cli:7.1", root = "cli-loader"),
   @Namespace(uri = "urn:infinispan:config:store:cli:7.2", root = "cli-loader"),
})
public class CLInterfaceLoaderConfigurationParser72 implements ConfigurationParser {

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case CLI_LOADER: {
            parseCliLoader(reader, builder.persistence());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }

   }

   private void parseCliLoader(XMLExtendedStreamReader reader,
         PersistenceConfigurationBuilder persistence)
         throws XMLStreamException {
      CLInterfaceLoaderConfigurationBuilder builder = new CLInterfaceLoaderConfigurationBuilder(persistence);
      parseCliLoaderAttributes(reader, builder);
      persistence.addStore(builder);
   }

   private void parseCliLoaderAttributes(XMLExtendedStreamReader reader,
         CLInterfaceLoaderConfigurationBuilder builder)
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
               Parser72.parseStoreAttribute(reader, i, builder);
               break;
            }
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

}
