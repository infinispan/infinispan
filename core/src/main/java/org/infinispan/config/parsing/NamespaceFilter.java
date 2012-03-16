/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.config.parsing;

import org.infinispan.Version;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * Adds namespace where needed when parsing an XML document
 */
public class NamespaceFilter extends XMLFilterImpl {

   public static final String ISPN_NS = "urn:infinispan:config:" + Version.MAJOR_MINOR;

   //State variable
   private boolean addedNamespace = false;

   @Override
   public void startDocument() throws SAXException {
      super.startDocument();
      startControlledPrefixMapping();
   }


   @Override
   public void startElement(String arg0, String arg1, String arg2,
                            Attributes arg3) throws SAXException {

      super.startElement(NamespaceFilter.ISPN_NS, arg1, arg2, arg3);
   }

   @Override
   public void endElement(String arg0, String arg1, String arg2)
           throws SAXException {

      super.endElement(NamespaceFilter.ISPN_NS, arg1, arg2);
   }

   @Override
   public void startPrefixMapping(String prefix, String url)
           throws SAXException {
      this.startControlledPrefixMapping();
   }

   private void startControlledPrefixMapping() throws SAXException {

      if (!this.addedNamespace) {
         //We should add namespace since it is set and has not yet been done.
         super.startPrefixMapping("", NamespaceFilter.ISPN_NS);

         //Make sure we dont do it twice
         this.addedNamespace = true;
      }
   }

}