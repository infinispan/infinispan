/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Encapsulates a chunk of cache entries that belong to the same segment. This representation is suitable for sending it
 * to another cache during state transfer.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateChunk {

   /**
    * The id of the segment for which we push cache entries.
    */
   private final int segmentId;

   /**
    * The cache entries. They are all guaranteed to be long to the same segment: segmentId.
    */
   private final Collection<InternalCacheEntry> cacheEntries;

   /**
    * Indicates to receiver if there are more chunks to come for this segment.
    */
   private final boolean isLastChunk;

   public StateChunk(int segmentId, Collection<InternalCacheEntry> cacheEntries, boolean isLastChunk) {
      this.segmentId = segmentId;
      this.cacheEntries = cacheEntries;
      this.isLastChunk = isLastChunk;
   }

   public int getSegmentId() {
      return segmentId;
   }

   public Collection<InternalCacheEntry> getCacheEntries() {
      return cacheEntries;
   }

   public boolean isLastChunk() {
      return isLastChunk;
   }

   @Override
   public String toString() {
      return "StateChunk{" +
            "segmentId=" + segmentId +
            ", cacheEntries=" + cacheEntries +
            ", isLastChunk=" + isLastChunk +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<StateChunk> {

      @Override
      public Integer getId() {
         return Ids.STATE_CHUNK;
      }

      @Override
      public Set<Class<? extends StateChunk>> getTypeClasses() {
         return Collections.<Class<? extends StateChunk>>singleton(StateChunk.class);
      }

      @Override
      public void writeObject(ObjectOutput output, StateChunk object) throws IOException {
         output.writeInt(object.segmentId);
         output.writeObject(object.cacheEntries);
         output.writeBoolean(object.isLastChunk);
      }

      @Override
      @SuppressWarnings("unchecked")
      public StateChunk readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int segmentId = input.readInt();
         Collection<InternalCacheEntry> cacheEntries = (Collection<InternalCacheEntry>) input.readObject();
         boolean isLastChunk = input.readBoolean();
         return new StateChunk(segmentId, cacheEntries, isLastChunk);
      }
   }
}
