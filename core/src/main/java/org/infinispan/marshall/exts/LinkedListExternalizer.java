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
package org.infinispan.marshall.exts;

import net.jcip.annotations.Immutable;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.MarshallUtil;
import org.infinispan.marshall.Marshalls;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;

/**
 * LinkedListExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
@Marshalls(typeClasses = LinkedList.class, id = Ids.LINKED_LIST)
public class LinkedListExternalizer implements Externalizer<LinkedList> {

   public void writeObject(ObjectOutput output, LinkedList list) throws IOException {
      MarshallUtil.marshallCollection(list, output);
   }

   public LinkedList readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int size = UnsignedNumeric.readUnsignedInt(input);
      LinkedList l = new LinkedList();
      for (int i = 0; i < size; i++) l.add(input.readObject());
      return l;
   }

}
