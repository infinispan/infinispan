/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.marshall.jboss;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import net.jcip.annotations.Immutable;

import org.infinispan.remoting.transport.jgroups.RequestIgnoredResponse;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * CustomObjectTable.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CustomObjectTable implements ObjectTable {
   
   private final List<Object> objects = new ArrayList<Object>();
   private final Map<Object, Writer> writers = new IdentityHashMap<Object, Writer>();
   private byte index;
   
   public void init() {
      objects.add(RequestIgnoredResponse.INSTANCE);
      writers.put(RequestIgnoredResponse.INSTANCE, new CustomObjectWriter(index++));
   }
   
   public void stop() {
      writers.clear();
      objects.clear();
   }
   
   public void add(Object o) {
      objects.add(o);
      writers.put(o, new CustomObjectWriter(index++));      
   }

   public Writer getObjectWriter(Object o) throws IOException {
      return writers.get(o);
   }

   public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
      return objects.get(unmarshaller.readUnsignedByte());
   }

   @Immutable
   static class CustomObjectWriter implements Writer {
      private final byte id;

      CustomObjectWriter(byte objectId) {
         this.id = objectId;
      }
      
      public void writeObject(Marshaller marshaller, Object object) throws IOException {
         marshaller.write(id);
      }      
   }
}
