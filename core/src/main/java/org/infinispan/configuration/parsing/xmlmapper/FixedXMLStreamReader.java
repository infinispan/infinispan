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

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class FixedXMLStreamReader implements XMLStreamReader {

    private final XMLStreamReader delegate;

    FixedXMLStreamReader(final XMLStreamReader delegate) {
        this.delegate = delegate;
    }

    @Override
   public Object getProperty(final String name) throws IllegalArgumentException {
        return delegate.getProperty(name);
    }

    @Override
   public int next() throws XMLStreamException {
        throw new UnsupportedOperationException();
    }

    @Override
   public void require(final int type, final String namespaceURI, final String localName) throws XMLStreamException {
        delegate.require(type, namespaceURI, localName);
    }

    @Override
   public String getElementText() throws XMLStreamException {
        return delegate.getElementText();
    }

    @Override
   public int nextTag() throws XMLStreamException {
        throw new UnsupportedOperationException();
    }

    @Override
   public boolean hasNext() throws XMLStreamException {
        return delegate.hasNext();
    }

    @Override
   public void close() throws XMLStreamException {
        throw new UnsupportedOperationException();
    }

    @Override
   public String getNamespaceURI(final String prefix) {
        return delegate.getNamespaceURI(prefix);
    }

    @Override
   public boolean isStartElement() {
        return delegate.isStartElement();
    }

    @Override
   public boolean isEndElement() {
        return delegate.isEndElement();
    }

    @Override
   public boolean isCharacters() {
        return delegate.isCharacters();
    }

    @Override
   public boolean isWhiteSpace() {
        return delegate.isWhiteSpace();
    }

    @Override
   public String getAttributeValue(final String namespaceURI, final String localName) {
        return delegate.getAttributeValue(namespaceURI, localName);
    }

    @Override
   public int getAttributeCount() {
        return delegate.getAttributeCount();
    }

    @Override
   public QName getAttributeName(final int index) {
        return delegate.getAttributeName(index);
    }

    @Override
   public String getAttributeNamespace(final int index) {
        return delegate.getAttributeNamespace(index);
    }

    @Override
   public String getAttributeLocalName(final int index) {
        return delegate.getAttributeLocalName(index);
    }

    @Override
   public String getAttributePrefix(final int index) {
        return delegate.getAttributePrefix(index);
    }

    @Override
   public String getAttributeType(final int index) {
        return delegate.getAttributeType(index);
    }

    @Override
   public String getAttributeValue(final int index) {
        return delegate.getAttributeValue(index);
    }

    @Override
   public boolean isAttributeSpecified(final int index) {
        return delegate.isAttributeSpecified(index);
    }

    @Override
   public int getNamespaceCount() {
        return delegate.getNamespaceCount();
    }

    @Override
   public String getNamespacePrefix(final int index) {
        return delegate.getNamespacePrefix(index);
    }

    @Override
   public String getNamespaceURI(final int index) {
        return delegate.getNamespaceURI(index);
    }

    @Override
   public NamespaceContext getNamespaceContext() {
        return delegate.getNamespaceContext();
    }

    @Override
   public int getEventType() {
        return delegate.getEventType();
    }

    @Override
   public String getText() {
        return delegate.getText();
    }

    @Override
   public char[] getTextCharacters() {
        return delegate.getTextCharacters();
    }

    @Override
   public int getTextCharacters(final int sourceStart, final char[] target, final int targetStart, final int length) throws XMLStreamException {
        return delegate.getTextCharacters(sourceStart, target, targetStart, length);
    }

    @Override
   public int getTextStart() {
        return delegate.getTextStart();
    }

    @Override
   public int getTextLength() {
        return delegate.getTextLength();
    }

    @Override
   public String getEncoding() {
        return delegate.getEncoding();
    }

    @Override
   public boolean hasText() {
        return delegate.hasText();
    }

    @Override
   public Location getLocation() {
        return delegate.getLocation();
    }

    @Override
   public QName getName() {
        return delegate.getName();
    }

    @Override
   public String getLocalName() {
        return delegate.getLocalName();
    }

    @Override
   public boolean hasName() {
        return delegate.hasName();
    }

    @Override
   public String getNamespaceURI() {
        return delegate.getNamespaceURI();
    }

    @Override
   public String getPrefix() {
        return delegate.getPrefix();
    }

    @Override
   public String getVersion() {
        return delegate.getVersion();
    }

    @Override
   public boolean isStandalone() {
        return delegate.isStandalone();
    }

    @Override
   public boolean standaloneSet() {
        return delegate.standaloneSet();
    }

    @Override
   public String getCharacterEncodingScheme() {
        return delegate.getCharacterEncodingScheme();
    }

    @Override
   public String getPITarget() {
        return delegate.getPITarget();
    }

    @Override
   public String getPIData() {
        return delegate.getPIData();
    }
}
