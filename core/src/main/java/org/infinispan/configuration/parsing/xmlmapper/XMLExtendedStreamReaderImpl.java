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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class XMLExtendedStreamReaderImpl implements XMLExtendedStreamReader {

    private final XMLMapperImpl xmlMapper;
    private final XMLStreamReader streamReader;
    private final XMLStreamReader fixedStreamReader;
    private final Deque<Context> stack = new ArrayDeque<Context>();
    private boolean trimElementText = true;

    XMLExtendedStreamReaderImpl(final XMLMapperImpl xmlMapper, final XMLStreamReader streamReader) {
        this.xmlMapper = xmlMapper;
        this.streamReader = streamReader;
        fixedStreamReader = new FixedXMLStreamReader(this.streamReader);
        stack.push(new Context());
    }

    @Override
    public void setTrimElementText(boolean trim) {
        this.trimElementText = trim;
    }

    @Override
    public void handleAny(final Object value) throws XMLStreamException {
        require(START_ELEMENT, null, null);
        boolean ok = false;
        try {
            final Deque<Context> stack = this.stack;
            stack.push(new Context());
            try {
                xmlMapper.processNested(this, value);
            } finally {
                stack.pop();
            }
            ok = true;
        } finally {
            if (! ok) {
                safeClose();
            }
        }
    }

    @Override
    public void handleAttribute(final Object value, final int index) throws XMLStreamException {
        require(START_ELEMENT, null, null);
        boolean ok = false;
        try {
            xmlMapper.processAttribute(fixedStreamReader, index, value);
        } finally {
            if (! ok) {
                safeClose();
            }
        }
    }

    @Override
    public void discardRemainder() throws XMLStreamException {
        final Context context = stack.getFirst();
        if (context.depth > 0) {
            try {
                doDiscard();
            } finally {
                context.depth--;
            }
        } else {
            try {
                throw readPastEnd(getLocation());
            } finally {
                safeClose();
            }
        }
    }

    @Override
    public Object getProperty(final String name) throws IllegalArgumentException {
        return streamReader.getProperty(name);
    }

    @Override
    public int next() throws XMLStreamException {
        final Context context = stack.getFirst();
        if (context.depth > 0) {
            final int next = streamReader.next();
            if (next == END_ELEMENT) {
                context.depth--;
            } else if(next == START_ELEMENT) {
                context.depth++;
            }
            return next;
        } else {
            try {
                throw readPastEnd(getLocation());
            } finally {
                safeClose();
            }
        }
    }

    @Override
    public void require(final int type, final String namespaceURI, final String localName) throws XMLStreamException {
        streamReader.require(type, namespaceURI, localName);
    }

    @Override
    public String getElementText() throws XMLStreamException {
        String text = streamReader.getElementText();
        return trimElementText ? text.trim() : text;
    }

    @Override
    public int nextTag() throws XMLStreamException {
        final Context context = stack.getFirst();
        if (context.depth > 0) {
            final int next = streamReader.nextTag();
            if (next == END_ELEMENT) {
                context.depth--;
            } else if(next == START_ELEMENT) {
                context.depth++;
            }
            return next;
        } else {
            try {
                throw readPastEnd(getLocation());
            } finally {
                safeClose();
            }
        }
    }

    @Override
    public boolean hasNext() throws XMLStreamException {
        return stack.getFirst().depth > 0 && streamReader.hasNext();
    }

    @Override
    public void close() throws XMLStreamException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNamespaceURI(final String prefix) {
        return streamReader.getNamespaceURI(prefix);
    }

    @Override
    public boolean isStartElement() {
        return streamReader.isStartElement();
    }

    @Override
    public boolean isEndElement() {
        return streamReader.isEndElement();
    }

    @Override
    public boolean isCharacters() {
        return streamReader.isCharacters();
    }

    @Override
    public boolean isWhiteSpace() {
        return streamReader.isWhiteSpace();
    }

    @Override
    public String getAttributeValue(final String namespaceURI, final String localName) {
        return streamReader.getAttributeValue(namespaceURI, localName);
    }

    @Override
    public int getAttributeCount() {
        return streamReader.getAttributeCount();
    }

    @Override
    public QName getAttributeName(final int index) {
        return streamReader.getAttributeName(index);
    }

    @Override
    public String getAttributeNamespace(final int index) {
        return streamReader.getAttributeNamespace(index);
    }

    @Override
    public String getAttributeLocalName(final int index) {
        return streamReader.getAttributeLocalName(index);
    }

    @Override
    public String getAttributePrefix(final int index) {
        return streamReader.getAttributePrefix(index);
    }

    @Override
    public String getAttributeType(final int index) {
        return streamReader.getAttributeType(index);
    }

    @Override
    public String getAttributeValue(final int index) {
        return streamReader.getAttributeValue(index);
    }

    @Override
    public boolean isAttributeSpecified(final int index) {
        return streamReader.isAttributeSpecified(index);
    }

    @Override
    public int getNamespaceCount() {
        return streamReader.getNamespaceCount();
    }

    @Override
    public String getNamespacePrefix(final int index) {
        return streamReader.getNamespacePrefix(index);
    }

    @Override
    public String getNamespaceURI(final int index) {
        return streamReader.getNamespaceURI(index);
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        return streamReader.getNamespaceContext();
    }

    @Override
    public int getEventType() {
        return streamReader.getEventType();
    }

    @Override
    public String getText() {
        return streamReader.getText();
    }

    @Override
    public char[] getTextCharacters() {
        return streamReader.getTextCharacters();
    }

    @Override
    public int getTextCharacters(final int sourceStart, final char[] target, final int targetStart, final int length) throws XMLStreamException {
        return streamReader.getTextCharacters(sourceStart, target, targetStart, length);
    }

    @Override
    public int getTextStart() {
        return streamReader.getTextStart();
    }

    @Override
    public int getTextLength() {
        return streamReader.getTextLength();
    }

    @Override
    public String getEncoding() {
        return streamReader.getEncoding();
    }

    @Override
    public boolean hasText() {
        return streamReader.hasText();
    }

    @Override
    public Location getLocation() {
        return streamReader.getLocation();
    }

    @Override
    public QName getName() {
        return streamReader.getName();
    }

    @Override
    public String getLocalName() {
        return streamReader.getLocalName();
    }

    @Override
    public boolean hasName() {
        return streamReader.hasName();
    }

    @Override
    public String getNamespaceURI() {
        return streamReader.getNamespaceURI();
    }

    @Override
    public String getPrefix() {
        return streamReader.getPrefix();
    }

    @Override
    public String getVersion() {
        return streamReader.getVersion();
    }

    @Override
    public boolean isStandalone() {
        return streamReader.isStandalone();
    }

    @Override
    public boolean standaloneSet() {
        return streamReader.standaloneSet();
    }

    @Override
    public String getCharacterEncodingScheme() {
        return streamReader.getCharacterEncodingScheme();
    }

    @Override
    public String getPITarget() {
        return streamReader.getPITarget();
    }

    @Override
    public String getPIData() {
        return streamReader.getPIData();
    }

    @Override
    public int getIntAttributeValue(final int index) throws XMLStreamException {
        try {
            return Integer.parseInt(getAttributeValue(index));
        } catch (NumberFormatException e) {
            throw intParseException(e, getLocation());
        }
    }

    @Override
    public int[] getIntListAttributeValue(final int index) throws XMLStreamException {
        try {
            return toInts(Spliterator.over(getAttributeValue(index), ' '), 0);
        } catch (NumberFormatException e) {
            throw intParseException(e, getLocation());
        }
    }

    @Override
    public List<String> getListAttributeValue(final int index) throws XMLStreamException {
        return Arrays.asList(toStrings(Spliterator.over(getAttributeValue(index), ' '), 0));
    }

    @Override
    public long getLongAttributeValue(final int index) throws XMLStreamException {
        try {
            return Long.parseLong(getAttributeValue(index));
        } catch (NumberFormatException e) {
            throw intParseException(e, getLocation());
        }
    }

    @Override
    public long[] getLongListAttributeValue(final int index) throws XMLStreamException {
        try {
            return toLongs(Spliterator.over(getAttributeValue(index), ' '), 0);
        } catch (NumberFormatException e) {
            throw intParseException(e, getLocation());
        }
    }

    @Override
    public <T> T getAttributeValue(final int index, final Class<T> kind) throws XMLStreamException {
        if (kind == String.class || kind == Object.class) {
            return kind.cast(getAttributeValue(index));
        } else if (kind == Integer.class || kind == Number.class) {
            return kind.cast(Integer.valueOf(getIntAttributeValue(index)));
        } else if (kind == Long.class) {
            return kind.cast(Long.valueOf(getLongAttributeValue(index)));
        } else if (kind.isEnum()) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            T value = (T) Enum.valueOf((Class<Enum>) kind, getAttributeValue(index));
            return value;
        } else if (kind == char[].class) {
            return kind.cast(getAttributeValue(index).toCharArray());
        } else {
            throw new XMLStreamException("Unknown value type of '" + kind + "'", getLocation());
        }
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public <T> List<? extends T> getListAttributeValue(final int index, final Class<T> kind) throws XMLStreamException {
        if (kind == String.class || kind == Object.class) {
            return (List<? extends T>) getListAttributeValue(index);
        } else if (kind == Integer.class || kind == Number.class) {
            final List<T> list = new ArrayList<T>();
            try {
                for (String s : Spliterable.over(getAttributeValue(index), ' ')) {
                    list.add(kind.cast(Integer.valueOf(s)));
                }
                return list;
            } catch (NumberFormatException e) {
                throw intParseException(e, getLocation());
            }
        } else if (kind == Long.class) {
            final List<T> list = new ArrayList<T>();
            try {
                for (String s : Spliterable.over(getAttributeValue(index), ' ')) {
                    list.add(kind.cast(Long.valueOf(s)));
                }
                return list;
            } catch (NumberFormatException e) {
                throw intParseException(e, getLocation());
            }
        } else if (kind.isEnum()) {
            final List<T> list = new ArrayList<T>();
            for (String s : Spliterable.over(getAttributeValue(index), ' ')) {
                list.add(kind.cast(Enum.valueOf(kind.asSubclass(Enum.class), s)));
            }
            return list;
        } else if (kind == char[].class) {
            final List<T> list = new ArrayList<T>();
            for (String s : Spliterable.over(getAttributeValue(index), ' ')) {
                list.add(kind.cast(s.toCharArray()));
            }
            return list;
        } else {
            throw new XMLStreamException("Unknown value type of '" + kind + "'", getLocation());
        }
    }

    @Override
    public String getId() throws XMLStreamException {
        return getAttributeValue(null, "id");
    }

    @Override
    public XMLMapper getXMLMapper() {
        return xmlMapper;
    }

    // private members

    private static final class Context {
        int depth = 1;
    }

    private void doDiscard() throws XMLStreamException {
        int i;
        while ((i = streamReader.next()) != END_ELEMENT) {
            if (i == START_ELEMENT) {
                doDiscard();
            }
        }
    }

    private void safeClose() {
        try {
            streamReader.close();
        } catch (Exception e) {
            // ignore
        }
    }

    private static final int[] NO_INTS = new int[0];
    private static final long[] NO_LONGS = new long[0];
    private static final String[] NO_STRINGS = new String[0];

    private static int[] toInts(Iterator<String> i, int count) {
        if (i.hasNext()) {
            final String n = i.next();
            final int[] ints = toInts(i, count + 1);
            ints[count] = Integer.parseInt(n);
            return ints;
        } else {
            return count == 0 ? NO_INTS : new int[count];
        }
    }

    private static long[] toLongs(Iterator<String> i, int count) {
        if (i.hasNext()) {
            final String n = i.next();
            final long[] longs = toLongs(i, count + 1);
            longs[count] = Long.parseLong(n);
            return longs;
        } else {
            return count == 0 ? NO_LONGS : new long[count];
        }
    }

    private static String[] toStrings(Iterator<String> i, int count) {
        if (i.hasNext()) {
            final String s = i.next();
            final String[] strings = toStrings(i, count + 1);
            strings[count] = s;
            return strings;
        } else {
            return count == 0 ? NO_STRINGS : new String[count];
        }
    }

    private static XMLStreamException readPastEnd(final Location location) {
        return new XMLStreamException("Attempt to read past end of element", location);
    }

    private static XMLStreamException intParseException(final NumberFormatException e, final Location location) {
        return new XMLStreamException("Failed to parse an integer attribute", location, e);
    }

}
