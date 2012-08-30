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
 * All valid attributes used to configure a MongoDB cachestore
 * Refer to {@link Element} to have the list of available configuration elements
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public enum Attribute {
   UNKNOWN(null),

   /**
    * Attributes of Element.CONNECTION
    */
   HOST("host"),
   PORT("port"),
   TIMEOUT("timeout"),
   ACKNOWLEDGMENT("acknowledgment"),

   /**
    * Attributes of Element.AUTHENTICATION
    */
   USERNAME("username"),
   PASSWORD("password"),

   /**
    * Attributes of Element.STORAGE
    */
   DATABASE("database"),
   COLLECTION("collection");

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * @return the name of the attribute
    */
   public String getName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }
}
