/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jpa.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser52;
import org.infinispan.util.StringPropertyReplacer;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * 
 */
public class JpaCacheStoreConfigurationParser53 implements
		ConfigurationParser<ConfigurationBuilderHolder> {

	public JpaCacheStoreConfigurationParser53() {
	}

	@Override
	public Namespace[] getSupportedNamespaces() {
		return new Namespace[] {
				new Namespace(Namespace.INFINISPAN_NS_BASE_URI, "jpa",
						Element.JPA_STORE.getLocalName(), 5, 2),
				new Namespace("", Element.JPA_STORE.getLocalName(), 0, 0) };
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
				Parser52.parseCommonStoreAttributes(reader, i, builder);
			}
			}
		}
	}
}
