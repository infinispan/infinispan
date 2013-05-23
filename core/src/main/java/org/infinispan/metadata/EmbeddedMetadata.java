/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.metadata;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Metadata class for embedded caches.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class EmbeddedMetadata implements Metadata {

   private final long lifespan;
   private final long maxIdle;
   private final EntryVersion version;

   private EmbeddedMetadata(
         long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit, EntryVersion version) {
      this.lifespan = lifespanUnit.toMillis(lifespan);
      this.maxIdle = maxIdleUnit.toMillis(maxIdle);
      this.version = version;
   }

   @Override
   public long lifespan() {
      return lifespan;
   }

   @Override
   public long maxIdle() {
      return maxIdle;
   }

   @Override
   public EntryVersion version() {
      return version;
   }

   @Override
   public Metadata.Builder builder() {
      // This method will be called rarely, so don't keep a reference!
      return new Builder().lifespan(lifespan).maxIdle(lifespan).version(version);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EmbeddedMetadata that = (EmbeddedMetadata) o;

      if (lifespan != that.lifespan) return false;
      if (maxIdle != that.maxIdle) return false;
      if (version != null ? !version.equals(that.version) : that.version != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (lifespan ^ (lifespan >>> 32));
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      result = 31 * result +  (version != null ? version.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "EmbeddedMetadata{" +
            "lifespan=" + lifespan +
            ", maxIdle=" + maxIdle +
            ", version=" + version +
            '}';
   }

   public static final class Builder implements Metadata.Builder {

      private long lifespan = -1;
      private TimeUnit lifespanUnit = TimeUnit.MILLISECONDS;
      private long maxIdle = -1;
      private TimeUnit maxIdleUnit = TimeUnit.MILLISECONDS;
      private EntryVersion version;

      @Override
      public Metadata.Builder lifespan(long time, TimeUnit unit) {
         lifespan = time;
         lifespanUnit = unit;
         return this;
      }

      @Override
      public Metadata.Builder lifespan(long time) {
         return lifespan(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Metadata.Builder maxIdle(long time, TimeUnit unit) {
         maxIdle = time;
         maxIdleUnit = unit;
         return this;
      }

      @Override
      public Metadata.Builder maxIdle(long time) {
         return maxIdle(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Metadata.Builder version(EntryVersion version) {
         this.version = version;
         return this;
      }

      @Override
      public Metadata build() {
         return new EmbeddedMetadata(
               lifespan, lifespanUnit, maxIdle, maxIdleUnit, version);
      }

   }

   public static class Externalizer extends AbstractExternalizer<EmbeddedMetadata> {

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends EmbeddedMetadata>> getTypeClasses() {
         return Util.<Class<? extends EmbeddedMetadata>>asSet(EmbeddedMetadata.class);
      }

      @Override
      public Integer getId() {
         return Ids.EMBEDDED_METADATA;
      }

      @Override
      public void writeObject(ObjectOutput output, EmbeddedMetadata object) throws IOException {
         output.writeLong(object.lifespan);
         output.writeLong(object.maxIdle);
         output.writeObject(object.version);
      }

      @Override
      public EmbeddedMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long lifespan = input.readLong();
         long maxIdle = input.readLong();
         EntryVersion version = (EntryVersion) input.readObject();
         return new EmbeddedMetadata(lifespan, TimeUnit.MILLISECONDS,
               maxIdle, TimeUnit.MILLISECONDS,
               version);
      }

   }

}
