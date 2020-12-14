package ${package};

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;

@Namespaces({
      // A version-specific parser for a cache store. If a parser is capable of parsing configuration for multiple versions
      // just add multiple @Namespace annotations, one for each version
      @Namespace(uri = "urn:infinispan:config:my-custom-store:0.0", root = "my-custom-store"),
      // The default parser. This namespace should be applied to the latest version of the parser
      @Namespace(root = "my-custom-store")
})
public class CustomStoreConfigurationParser implements ConfigurationParser {

   @Override
   public Namespace[] getNamespaces() {
      /*
       * Return the namespaces for which this parser should be used.
       */
      return ParseUtils.getNamespaceAnnotations(this.getClass());
   }

   @Override
   public void readElement(ConfigurationReader reader,
                           ConfigurationBuilderHolder configurationHolder) {
      ConfigurationBuilder builder = configurationHolder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case SAMPLE_ELEMENT: {
            parseSampleElement(reader, builder.persistence());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseSampleElement(ConfigurationReader reader, PersistenceConfigurationBuilder persistenceBuilder) {
      CustomStoreConfigurationBuilder storeBuilder = new CustomStoreConfigurationBuilder(persistenceBuilder);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case SAMPLE_ATTRIBUTE: {
               storeBuilder.sampleAttribute(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   enum Element {
      // must be first
      UNKNOWN(null),

      SAMPLE_ELEMENT("sample-element");

      private final String name;

      Element(final String name) {
         this.name = name;
      }

      /**
       * Get the local name of this element.
       *
       * @return the local name
       */
      public String getLocalName() {
         return name;
      }

      private static final Map<String, Element> MAP;

      static {
         final Map<String, Element> map = new HashMap<>(8);
         for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) {
               map.put(name, element);
            }
         }
         MAP = map;
      }

      public static Element forName(final String localName) {
         final Element element = MAP.get(localName);
         return element == null ? UNKNOWN : element;
      }
   }

   enum Attribute {

      // must be first
      UNKNOWN(null),

      SAMPLE_ATTRIBUTE("sample-attribute");

      // Other enums to be placed here

      private final String name;

      Attribute(final String name) {
         this.name = name;
      }

      /**
       * Get the local name of this element.
       *
       * @return the local name
       */
      public String getLocalName() {
         return name;
      }

      private static final Map<String, Attribute> attributes;

      static {
         final Map<String, Attribute> map = new HashMap<>();
         for (Attribute attribute : values()) {
            final String name = attribute.getLocalName();
            if (name != null) {
               map.put(name, attribute);
            }
         }
         attributes = map;
      }

      public static Attribute forName(final String localName) {
         final Attribute attribute = attributes.get(localName);
         return attribute == null ? UNKNOWN : attribute;
      }
   }
}
