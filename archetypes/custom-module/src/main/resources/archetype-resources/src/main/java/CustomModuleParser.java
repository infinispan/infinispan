package ${package};

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.ParserScope;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@Namespace(root = "custom-module")
@Namespace(uri = Parser.NAMESPACE + "*", root = "custom-module")
public class CustomModuleParser implements ConfigurationParser {

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.CACHE_CONTAINER))
         throw new CacheConfigurationException(String.format("Unexpected scope. Expected CACHE_CONTAINER but was %s", holder.getScope()));


      Element element = Element.forName(reader.getLocalName());
      if (element != Element.ROOT)
         throw ParseUtils.unexpectedElement(reader);

      GlobalConfigurationBuilder globalBuilder = holder.getGlobalConfigurationBuilder();
      CustomModuleConfigurationBuilder builder = globalBuilder.addModule(CustomModuleConfigurationBuilder.class);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case MESSAGE:
               builder.message(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
