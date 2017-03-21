package org.infinispan.configuration.internal;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * A {@link ConfigurationSerializer} implementation for {@link PrivateGlobalConfiguration}.
 * <p>
 * The {@link PrivateGlobalConfiguration} is only to be used internally so this implementation is a no-op.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrivateGlobalConfigurationSerializer implements ConfigurationSerializer<PrivateGlobalConfiguration> {
   @Override
   public void serialize(XMLExtendedStreamWriter writer, PrivateGlobalConfiguration configuration) throws XMLStreamException {
      //nothing to do! private configuration is not serialized.
   }
}
