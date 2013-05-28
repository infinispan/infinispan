/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.atomic;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * DeltaCompositeKey is the key guarding access to a specific entry in DeltaAware
 */
public final class DeltaCompositeKey {

   private final Object deltaAwareValueKey;
   private final Object entryKey;

   public DeltaCompositeKey(Object deltaAwareValueKey, Object entryKey) {
      if (deltaAwareValueKey == null || entryKey == null)
         throw new IllegalArgumentException("Keys cannot be null");

      this.deltaAwareValueKey = deltaAwareValueKey;
      this.entryKey = entryKey;
   }

   public final Object getDeltaAwareValueKey() {
      return deltaAwareValueKey;
   }

   @Override
   public int hashCode() {
      return 31 * deltaAwareValueKey.hashCode() + entryKey.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof DeltaCompositeKey)) {
         return false;
      }
      DeltaCompositeKey other = (DeltaCompositeKey) obj;
      return deltaAwareValueKey.equals(other.deltaAwareValueKey) && entryKey.equals(other.entryKey);
   }

   @Override
   public String toString() {
      return "DeltaCompositeKey[deltaAwareValueKey=" + deltaAwareValueKey + ", entryKey=" + entryKey + ']';
   }

   public static class DeltaCompositeKeyExternalizer extends AbstractExternalizer<DeltaCompositeKey> {

      @Override
      public void writeObject(ObjectOutput output, DeltaCompositeKey dck) throws IOException {
         output.writeObject(dck.deltaAwareValueKey);
         output.writeObject(dck.entryKey);
      }

      @Override
      @SuppressWarnings("unchecked")
      public DeltaCompositeKey readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         Object deltaAwareValueKey = unmarshaller.readObject();
         Object entryKey = unmarshaller.readObject();
         return new DeltaCompositeKey(deltaAwareValueKey, entryKey);
      }

      @Override
      public Integer getId() {
         return Ids.DELTA_COMPOSITE_KEY;
      }

      @Override
      public Set<Class<? extends DeltaCompositeKey>> getTypeClasses() {
         return Collections.<Class<? extends DeltaCompositeKey>>singleton(DeltaCompositeKey.class);
      }
   }
}
