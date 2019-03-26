package org.infinispan.commands.module;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * A {@link ConfigurationSerializer} implementation for {@link TestGlobalConfiguration}.
 * <p>
 * The {@link TestGlobalConfiguration} is only to be used internally so this implementation is a no-op.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TestGlobalConfigurationSerializer implements ConfigurationSerializer<TestGlobalConfiguration> {
   @Override
   public void serialize(XMLExtendedStreamWriter writer, TestGlobalConfiguration configuration) throws XMLStreamException {
      //nothing to do! test configuration is not serialized.
   }
}
