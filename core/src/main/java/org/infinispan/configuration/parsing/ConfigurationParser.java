/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.configuration.parsing;

import javax.xml.stream.XMLStreamException;


/**
 *
 * ConfigurationParser.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConfigurationParser {

   /**
    * The entry point of a configuration parser which gets passed a {@link XMLExtendedStreamReader} positioned at a root
    * element associated with the parser itself according to the registered mapping.
    *
    * @param reader the XML stream reader
    * @param holder a holder object used by the parser to maintain state
    * @throws XMLStreamException
    */
   void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException;
}
