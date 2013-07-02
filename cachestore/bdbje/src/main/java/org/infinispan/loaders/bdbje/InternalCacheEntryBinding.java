package org.infinispan.loaders.bdbje;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.commons.marshall.StreamingMarshaller;

import java.io.IOException;

class InternalCacheEntryBinding implements EntryBinding<InternalCacheEntry> {
   StreamingMarshaller m;

   InternalCacheEntryBinding(StreamingMarshaller m) {
      this.m = m;
   }

   @Override
   public InternalCacheEntry entryToObject(DatabaseEntry entry) {
      try {
         return (InternalCacheEntry) m.objectFromByteBuffer(entry.getData());
      } catch (IOException e) {
         throw new RuntimeExceptionWrapper(e);
      } catch (ClassNotFoundException e) {
         throw new RuntimeExceptionWrapper(e);
      }
   }

   @Override
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
