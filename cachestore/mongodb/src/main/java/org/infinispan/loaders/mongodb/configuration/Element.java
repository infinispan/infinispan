/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.loaders.mongodb.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * All valid elements to configure a MongoDB cachestore
 * See also {@link Attribute} to have the complete list of attributes
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public enum Element {
   UNKNOWN(null),
   MONGODB_STORE("mongodbStore"),
   CONNECTION("connection"),
   AUTHENTICATION("authentication"),
   STORAGE("storage");

   private final String name;


   Element(final String name) {
      this.name = name;
   }

   /**
    * Get the name of the current element
    *
    * @return the name
    */
   public String getName() {
      return name;
   }

   private static final Map<String, Element> elements;

   static {
      final Map<String, Element> map = new HashMap<String, Element>(8);
      for (Element element : values()) {
         final String name = element.getName();
         if (name != null) {
            map.put(name, element);
         }
      }
      elements = map;
   }

   public static Element forName(final String localName) {
      final Element element = elements.get(localName);
      return element == null ? UNKNOWN : element;
   }
}
