package org.infinispan.configuration.parsing;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.StoreConfigurationBuilder;

/**
 * This class just acts as a bridge to the unified {@link Parser} for external cache stores. All uses of it should be
 * removed in favour of that.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

@Deprecated
public class Parser72 {
   public static void parseStoreElement(XMLExtendedStreamReader reader, StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      Parser.parseStoreElement(reader, storeBuilder);
   }
}
