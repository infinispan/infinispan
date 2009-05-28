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
package org.infinispan.marshall.jboss.externalizers;

import org.infinispan.marshall.jboss.MarshallUtil;
import org.infinispan.marshall.jboss.Externalizer;
import org.infinispan.util.FastCopyHashMap;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Map externalizer for all map implementations except immutable maps and singleton maps, i.e. FastCopyHashMap, HashMap,
 * TreeMap.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MapExternalizer implements Externalizer {
   private static final int HASHMAP = 0;
   private static final int TREEMAP = 1;
   private static final int FASTCOPYHASHMAP = 2;
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(3);
   
   public MapExternalizer() {
      numbers.put(HashMap.class, HASHMAP);
      numbers.put(TreeMap.class, TREEMAP);
      numbers.put(FastCopyHashMap.class, FASTCOPYHASHMAP);
   }

   public void writeObject(Marshaller output, Object subject) throws IOException {
      int number = numbers.get(subject.getClass(), -1);
      output.writeByte(number);
      MarshallUtil.marshallMap((Map) subject, output);
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      Map subject = null;
      switch (magicNumber) {
         case HASHMAP:
            subject = new HashMap();
            break;
         case TREEMAP:
            subject = new TreeMap();
            break;
         case FASTCOPYHASHMAP:
            subject = new FastCopyHashMap();
            break;
      }
      MarshallUtil.unmarshallMap(subject, input);
      return subject;
   }
}
