/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.configuration.parsing.xmlmapper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import static javax.xml.stream.XMLStreamConstants.*;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class XMLMapperImpl implements XMLMapper {
    private final ConcurrentMap<QName, XMLElementReader<?>> rootElements = new ConcurrentHashMap<QName, XMLElementReader<?>>();
    private final ConcurrentMap<QName, XMLAttributeReader<?>> rootAttributes = new ConcurrentHashMap<QName, XMLAttributeReader<?>>();

    @Override
   public void registerRootElement(QName name, XMLElementReader<?> reader) {
        if (rootElements.putIfAbsent(name, reader) != null) {
            throw new IllegalArgumentException("Root element for " + name + " already registered");
        }
    }

    @Override
    public void unregisterRootElement(QName name) {
        rootElements.remove(name);
    }

    @Override
   public void registerRootAttribute(QName name, XMLAttributeReader<?> reader) {
        if (rootAttributes.putIfAbsent(name, reader) != null) {
            throw new IllegalArgumentException("Root attribute for " + name + " already registered");
        }
    }

    @Override
    public void unregisterRootAttribute(QName name) {
        rootAttributes.remove(name);
    }

    @Override
   public void parseDocument(Object rootObject, XMLStreamReader reader) throws XMLStreamException {
        try {
            reader.require(START_DOCUMENT, null, null);
            reader.nextTag();
            reader.require(START_ELEMENT, null, null);
            processNested(new XMLExtendedStreamReaderImpl(this, reader), rootObject);
            while (reader.next() != END_DOCUMENT) {
            }
            reader.close();
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                // log it?
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    <T> void processNested(final XMLExtendedStreamReader streamReader, final T value) throws XMLStreamException {
        final QName name = streamReader.getName();
        final XMLElementReader<T> reader = (XMLElementReader<T>) rootElements.get(name);
        if (reader == null) {
            throw new XMLStreamException("Unexpected element '" + name + "'", streamReader.getLocation());
        }
        reader.readElement(streamReader, value);
    }

    @SuppressWarnings({ "unchecked" })
    <T> void processAttribute(final XMLStreamReader streamReader, final int index, final T value) throws XMLStreamException {
        final QName name = streamReader.getName();
        final XMLAttributeReader<T> reader = (XMLAttributeReader<T>) rootAttributes.get(name);
        if (reader == null) {
            throw new XMLStreamException("Unexpected attribute '" + name + "'", streamReader.getLocation());
        }
        reader.readAttribute(streamReader, index, value);
    }
}
