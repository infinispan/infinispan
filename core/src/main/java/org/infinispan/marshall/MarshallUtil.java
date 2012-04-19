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
package org.infinispan.marshall;

import net.jcip.annotations.Immutable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.io.UnsignedNumeric;

/**
 * MarshallUtil.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class MarshallUtil {

   public static void marshallCollection(Collection<?> c, ObjectOutput out) throws IOException {
      UnsignedNumeric.writeUnsignedInt(out, c.size());
      for (Object o : c) {
         out.writeObject(o);
      }
   }

   public static void marshallMap(Map<?, ?> map, ObjectOutput out) throws IOException {
      int mapSize = map.size();
      UnsignedNumeric.writeUnsignedInt(out, mapSize);
      if (mapSize == 0) return;

      for (Map.Entry<?, ?> me : map.entrySet()) {
         out.writeObject(me.getKey());
         out.writeObject(me.getValue());
      }
   }

   public static void unmarshallMap(Map<Object, Object> map, ObjectInput in) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(in);
      for (int i = 0; i < size; i++) map.put(in.readObject(), in.readObject());
   }
}
