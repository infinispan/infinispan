package org.infinispan.multimap.configuration;

import static org.infinispan.multimap.configuration.MultimapConfigurationParser.NAMESPACE;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Map;

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
@Namespace(root = "multimaps")
@Namespace(uri = NAMESPACE + "*", root = "multimaps", since = "15.0")
public class MultimapConfigurationParser implements ConfigurationParser {

    static final String NAMESPACE = Parser.NAMESPACE + "multimaps:";

    @Override
    public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
        if (!holder.inScope(ParserScope.CACHE_CONTAINER)) {
            throw CONTAINER.invalidScope(ParserScope.CACHE_CONTAINER.name(), holder.getScope());
        }

        GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
        Element element = Element.forName(reader.getLocalName());

        if (element != Element.MULTIMAPS) throw ParseUtils.unexpectedElement(reader);

        parseMultimaps(reader, builder.addModule(MultimapCacheManagerConfigurationBuilder.class));
        reader.require(ConfigurationReader.ElementType.END_ELEMENT, null, Element.MULTIMAPS);
    }

    @Override
    public Namespace[] getNamespaces() {
        return ParseUtils.getNamespaceAnnotations(getClass());
    }

    private void parseMultimaps(ConfigurationReader reader, MultimapCacheManagerConfigurationBuilder builder) {
        while (reader.inTag()) {
            Map.Entry<String, String> item = reader.getMapItem(Attribute.NAME);
            Element element = Element.forName(item.getValue());
            switch (element) {
                case MULTIMAP: {
                    parseMultimapElement(reader, builder.addMultimap().name(item.getKey()));
                    break;
                }
                default:
                    throw ParseUtils.unexpectedElement(reader);
            }
            reader.endMapItem();
        }
    }

    private void parseMultimapElement(ConfigurationReader reader, EmbeddedMultimapConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = reader.getAttributeValue(i);
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case SUPPORTS_DUPLICATES:
                    builder.supportsDuplicates(Boolean.parseBoolean(value));
                    break;
                case NAME:
                    // Already defined.
                    break;
                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }
        ParseUtils.requireNoContent(reader);
    }
}
