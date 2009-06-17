/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
 *
 */

package org.infinispan.tree;

import org.infinispan.util.Util;

/**
 * A class that represents the key to a node
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NodeKey {
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
}
