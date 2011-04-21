/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

/**
 * The purpose of this class is to parse the jgroups configuration from the config file into an compact string that can
 * be passed as a config to the channel.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class JGroupsStackParser {
   /**
    * Parses the cluster config which is used to start a JGroups channel
    *
    * @param config an old-style JGroups protocol config String
    */
   public String parseClusterConfigXml(Element config) {
      StringBuilder buffer = new StringBuilder();
      NodeList stack = config.getChildNodes();
      int length = stack.getLength();

      for (int s = 0; s < length; s++) {
         org.w3c.dom.Node node = stack.item(s);
         if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
            continue;
         }

         // Ignore Namespace until JGroups defines one
         Element tag = (Element) node;
         String protocol = tag.getLocalName();
         if (protocol == null) {
            protocol = tag.getNodeName(); // try a non-namespace aware version
         }
         buffer.append(protocol);
         processAttributes(buffer, tag);
         buffer.append(':');
      }
      if (buffer.length() > 0) {
         //Remove the trailing ':'
         buffer.setLength(buffer.length() - 1);
      }
      return buffer.toString();
   }

   private void processAttributes(StringBuilder buffer, Element tag) {
      NamedNodeMap attrs = tag.getAttributes();
      int attrLength = attrs.getLength();
      if (attrLength > 0) {
         buffer.append('(');
      }
      for (int a = 0; a < attrLength; a++) {
         Attr attr = (Attr) attrs.item(a);
         processSingleAttribute(buffer, attr);
         if (a < attrLength - 1) {
            buffer.append(';');
         }
      }
      if (attrLength > 0) {
         buffer.append(')');
      }
   }

   private void processSingleAttribute(StringBuilder buffer, Attr attr) {
      String name = attr.getName();
      String value = attr.getValue();
      buffer.append(name);
      buffer.append('=');
      buffer.append(value);
   }
}
