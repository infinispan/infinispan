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
package org.infinispan.commons.marshall;

import java.util.ArrayList;

import org.infinispan.api.CacheException;

/**
 * An efficient array-based list of referenced objects, using the reference id as a subscript for the array.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class UnmarshalledReferences {
   private final ArrayList<Object> referencedObjects = new ArrayList<Object>();

   /**
    * Retrieves an object referenced by an id
    *
    * @param ref reference
    * @return object
    */
   public Object getReferencedObject(int ref) {
      if (ref >= referencedObjects.size())
         throw new CacheException("Attempting to look up a ref that hasn't been inserted yet");
      return referencedObjects.get(ref);
   }

   /**
    * Adds a referenced object to the list of references
    *
    * @param ref reference id
    * @param o   object
    */
   public void putReferencedObject(int ref, Object o) {
      int sz = referencedObjects.size();
      // if we are not adding the object to the end of the list, make sure we use a specific position
      if (ref < sz) {
         referencedObjects.set(ref, o);
         return;
      } else if (ref > sz) {
         // if we are adding the reference to a position beyond the end of the list, make sure we expand the list first.
         for (int i = sz; i < ref; i++) referencedObjects.add(null);
      }
      referencedObjects.add(o);
   }
}
