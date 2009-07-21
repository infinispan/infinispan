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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.jcip.annotations.Immutable;

import org.infinispan.container.entries.InternalCacheEntry;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.loaders.bucket.Bucket;
import org.infinispan.marshall.Externalizer;

/**
 * BucketExternalizer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Externalizer implementation now within {@link Bucket}
 */
@Immutable
public class BucketExternalizer implements Externalizer {

   public void writeObject(ObjectOutput output, Object subject) throws IOException {
      Bucket b = (Bucket) subject;
      UnsignedNumeric.writeUnsignedInt(output, b.getNumEntries());
      for (InternalCacheEntry se : b.getEntries().values()) output.writeObject(se);
   }

   public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Bucket b = new Bucket();
      int numEntries = UnsignedNumeric.readUnsignedInt(input);
      for (int i = 0; i < numEntries; i++) b.addEntry((InternalCacheEntry) input.readObject());
      return b;
   }

}
