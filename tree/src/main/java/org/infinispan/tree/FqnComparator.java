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

import net.jcip.annotations.Immutable;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares the order of two FQN. Sorts by name, then by depth, e.g.
 * <pre>
 * aaa/bbb
 * xxx
 * xxx/ccc
 * </pre>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 * @since 4.0
 */
@Immutable
public class FqnComparator implements Comparator<Fqn>, Serializable {
   public static final FqnComparator INSTANCE = new FqnComparator();
   private static final long serialVersionUID = -1357631755443829281L;

   /**
    * Returns -1 if the first comes before; 0 if they are the same; 1 if the second Fqn comes before.  <code>null</code>
    * always comes first.
    */
   @Override
   public int compare(Fqn fqn1, Fqn fqn2) {
      int s1 = fqn1.size();
      int s2 = fqn2.size();

      if (s1 == 0) {
         return (s2 == 0) ? 0 : -1;
      }

      if (s2 == 0) {
         return 1;
      }

//      if (fqn1.getClass().equals(StringFqn.class) && fqn2.getClass().equals(StringFqn.class))
//      {
//         StringFqn sfqn1 = (StringFqn) fqn1;
//         StringFqn sfqn2 = (StringFqn) fqn2;
//         return sfqn1.stringRepresentation.compareTo(sfqn2.stringRepresentation);
//      }
      int size = Math.min(s1, s2);

      for (int i = 0; i < size; i++) {
         Object e1 = fqn1.get(i);
         Object e2 = fqn2.get(i);
         if (e1 == e2) {
            continue;
         }
         if (e1 == null) {
            return 0;
         }
         if (e2 == null) {
            return 1;
         }
         if (!e1.equals(e2)) {
            int c = compareElements(e1, e2);
            if (c != 0) {
               return c;
            }
         }
      }

      return s1 - s2;
   }

   /**
    * Compares two Fqn elements. If e1 and e2 are the same class and e1 implements Comparable, returns e1.compareTo(e2).
    * Otherwise, returns e1.toString().compareTo(e2.toString()).
    */
   @SuppressWarnings("unchecked")
   private int compareElements(Object e1, Object e2) {
      if (e1.getClass() == e2.getClass() && e1 instanceof Comparable) {
         return ((Comparable<Object>) e1).compareTo(e2);
      } else {
         return e1.toString().compareTo(e2.toString());
      }
   }


}