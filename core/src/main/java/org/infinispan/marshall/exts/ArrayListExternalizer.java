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
package org.infinispan.marshall.exts;

import net.jcip.annotations.Immutable;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.MarshallUtil;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Set;

/**
 * List externalizer dealing with ArrayList and LinkedList implementations.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class ArrayListExternalizer extends AbstractExternalizer<ArrayList> {

   @Override
   public void writeObject(ObjectOutput output, ArrayList list) throws IOException {
      MarshallUtil.marshallCollection(list, output);
   }

   @Override
   public ArrayList readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(input);
      ArrayList l = new ArrayList(size);
      for (int i = 0; i < size; i++) l.add(input.readObject());
      return l;
   }

   @Override
   public Integer getId() {
      return Ids.ARRAY_LIST;
   }

   @Override
   public Set<Class<? extends ArrayList>> getTypeClasses() {
      return Util.<Class<? extends ArrayList>>asSet(ArrayList.class);
   }
}
