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
 */
package org.infinispan.atomic;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.atomic.Operation;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

/**
 * Changes that have occurred on an AtomicHashMap
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
@Marshallable(externalizer = AtomicHashMapDelta.Externalizer.class, id = Ids.ATOMIC_HASH_MAP_DELTA)
public class AtomicHashMapDelta implements Delta {
   private static final Log log = LogFactory.getLog(AtomicHashMapDelta.class);
   private static final boolean trace = log.isTraceEnabled();

   private List<Operation> changelog;

   public DeltaAware merge(DeltaAware d) {
      AtomicHashMap other;
      if (d != null && (d instanceof AtomicHashMap))
         other = (AtomicHashMap) d;
      else
         other = new AtomicHashMap();

      for (Operation o : changelog) o.replay(other.delegate);
      other.commit();
      return other;
   }

   public void addOperation(Operation o) {
      if (changelog == null) {
         // lazy init
         changelog = new LinkedList<Operation>();
      }
      changelog.add(o);
   }

   @Override
   public String toString() {
      return "AtomicHashMapDelta{" +
            "changelog=" + changelog +
            '}';
   }

   public int getChangeLogSize() {
      return changelog == null ? 0 : changelog.size();
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         AtomicHashMapDelta delta = (AtomicHashMapDelta) object;        
         if (trace) log.trace("Serializing changelog " + delta.changelog);
         output.writeObject(delta.changelog);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         AtomicHashMapDelta delta = new AtomicHashMapDelta();
         delta.changelog = (List<Operation>) input.readObject();
         if (trace) log.trace("Deserialized changelog " + delta.changelog);
         return delta;
      }
   }
}