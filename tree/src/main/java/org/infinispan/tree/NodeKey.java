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

package org.infinispan.tree;

import static org.infinispan.tree.NodeKey.Type.DATA;
import static org.infinispan.tree.NodeKey.Type.STRUCTURE;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.util.Util;

/**
 * A class that represents the key to a node
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NodeKey implements Serializable {
   final Fqn fqn;
   final Type contents;

   public static enum Type {
      DATA, STRUCTURE
   }

   public NodeKey(Fqn fqn, Type contents) {
      this.contents = contents;
      this.fqn = fqn;
   }
   
   public Fqn getFqn() {
      return fqn;
   }

   public Type getContents() {
      return contents;
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NodeKey key = (NodeKey) o;

      if (contents != key.contents) return false;
      if (!Util.safeEquals(fqn, key.fqn)) return false;

      return true;
   }

   public int hashCode() {
      int h = fqn != null ? fqn.hashCode() : 1;
      h += ~(h << 9);
      h ^= (h >>> 14);
      h += (h << 4);
      h ^= (h >>> 10);
      return h;
   }

   public String toString() {
      return "NodeKey{" +
            "contents=" + contents +
            ", fqn=" + fqn +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<NodeKey> {
      private static final byte DATA_BYTE = 1;
      private static final byte STRUCTURE_BYTE = 2;

      @Override
      public void writeObject(ObjectOutput output, NodeKey key) throws IOException {
         output.writeObject(key.fqn);
         byte type = 0;
         switch (key.contents) {
            case DATA:
               type = DATA_BYTE;
               break;
            case STRUCTURE:
               type = STRUCTURE_BYTE;
               break;
         }
         output.write(type);
      }
      
      @Override
      public NodeKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Fqn fqn = (Fqn) input.readObject();
         int typeb = input.readUnsignedByte();
         NodeKey.Type type = null; 
         switch (typeb) {
            case DATA_BYTE:
               type = DATA;
               break;
            case STRUCTURE_BYTE:
               type = STRUCTURE;
               break;
         }
         return new NodeKey(fqn, type);
      }

      @Override
      public Set<Class<? extends NodeKey>> getTypeClasses() {
         return Util.<Class<? extends NodeKey>>asSet(NodeKey.class);
      }
   }
}
