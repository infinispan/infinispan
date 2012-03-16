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
package org.infinispan.loaders.bdbje;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.StreamingMarshaller;

import java.io.IOException;

class InternalCacheEntryBinding implements EntryBinding<InternalCacheEntry> {
   StreamingMarshaller m;

   InternalCacheEntryBinding(StreamingMarshaller m) {
      this.m = m;
   }

   public InternalCacheEntry entryToObject(DatabaseEntry entry) {
      try {
         return (InternalCacheEntry) m.objectFromByteBuffer(entry.getData());
      } catch (IOException e) {
         throw new RuntimeExceptionWrapper(e);
      } catch (ClassNotFoundException e) {
         throw new RuntimeExceptionWrapper(e);
      }
   }

   public void objectToEntry(InternalCacheEntry object, DatabaseEntry entry) {
      byte[] b;
      try {
         b = m.objectToByteBuffer(object);
      } catch (IOException e) {
         throw new RuntimeExceptionWrapper(e);
      } catch (InterruptedException ie) {
         Thread.currentThread().interrupt();
         throw new RuntimeExceptionWrapper(ie);
      }
      entry.setData(b);
   }
}
