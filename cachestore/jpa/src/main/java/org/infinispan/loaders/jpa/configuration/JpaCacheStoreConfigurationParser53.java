package org.infinispan.loaders.jpa.configuration;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser53;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.commons.util.StringPropertyReplacer;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:jpa:5.3", root = "jpaStore"),
})
public class JpaCacheStoreConfigurationParser53 implements
		ConfigurationParser {

	public JpaCacheStoreConfigurationParser53() {
	}

	@Override
   public void readElement(XMLExtendedStreamReader reader,
			ConfigurationBuilderHolder holder) throws XMLStreamException {
		ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
		Element element = Element.forName(reader.getLocalName());
		switch (element) {
		case JPA_STORE: {
			parseJpaCacheStore(
					reader,
					builder.loaders().addLoader(
							JpaCacheStoreConfigurationBuilder.class));
			break;
		}
		default: {
			throw ParseUtils.unexpectedElement(reader);
		}
		}
	}

	private void parseJpaCacheStore(XMLExtendedStreamReader reader,
			JpaCacheStoreConfigurationBuilder builder)
			throws XMLStreamException {
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			ParseUtils.requireNoNamespaceAttribute(reader, i);
			String value = StringPropertyReplacer.replaceProperties(reader
					.getAttributeValue(i));
			Attribute attribute = Attribute.forName(reader
					.getAttributeLocalName(i));

			switch (attribute) {
			case ENTITY_CLASS_NAME: {
				Class<?> clazz;
				try {
					clazz = this.getClass().getClassLoader().loadClass(value);
				} catch (ClassNotFoundException e) {
					throw new XMLStreamException("Class " + value
							+ " specified in entityClassName is not found", e);
				}
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
			default: {
                Parser53.parseCommonStoreAttributes(reader, i, builder);
			}
			}
		}

		if (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         ParseUtils.unexpectedElement(reader);
      }
	}
}
