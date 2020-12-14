package org.infinispan.configuration.serializing;

import static org.infinispan.configuration.serializing.SerializeUtils.writeTypedProperties;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
/**
 * AbstractStoreSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public abstract class AbstractStoreSerializer {
   protected void writeCommonStoreSubAttributes(ConfigurationWriter writer, AbstractStoreConfiguration configuration) {
   }

   protected void writeCommonStoreElements(ConfigurationWriter writer, StoreConfiguration configuration) {
      if (configuration.async().enabled()) {
         writeStoreWriteBehind(writer, configuration);
      }
      writeTypedProperties(writer, TypedProperties.toTypedProperties(configuration.properties()));
   }

   private void writeStoreWriteBehind(ConfigurationWriter writer, StoreConfiguration configuration) {
      AttributeSet writeBehind = configuration.async().attributes();
      if (writeBehind.isModified()) {
         writer.writeStartElement(Element.WRITE_BEHIND);
         writeBehind.write(writer, AsyncStoreConfiguration.MODIFICATION_QUEUE_SIZE, Attribute.MODIFICATION_QUEUE_SIZE);
         writeBehind.write(writer, AsyncStoreConfiguration.FAIL_SILENTLY, Attribute.FAIL_SILENTLY);
         writer.writeEndElement();
      }
   }

}
