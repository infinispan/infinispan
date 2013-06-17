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

package org.infinispan.container.versioning;

import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * This is a version set to the MVCC entries when they read a key and the keys does not exists.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public final class NonExistingVersion implements IncrementableEntryVersion {

   public static final NonExistingVersion INSTANCE = new NonExistingVersion();

   //private constructor.
   private NonExistingVersion() {
   }

   @Override
   public final InequalVersionComparisonResult compareTo(EntryVersion other) {
      return other == INSTANCE ? InequalVersionComparisonResult.EQUAL : InequalVersionComparisonResult.BEFORE;
   }

   @Override
   public String toString() {
      return "NON_EXISTING_VERSION";
   }

   public static class Externalizer implements AdvancedExternalizer<NonExistingVersion> {

      @Override
      public Set<Class<? extends NonExistingVersion>> getTypeClasses() {
         return Collections.<Class<? extends NonExistingVersion>>singleton(NonExistingVersion.class);
      }

      @Override
      public Integer getId() {
         return Ids.NON_EXISTING_VERSION;
      }

      @Override
      public void writeObject(ObjectOutput output, NonExistingVersion object) throws IOException {
         //no-op, singleton!
      }

      @Override
      public NonExistingVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         //singleton!
         return INSTANCE;
      }
   }
}
