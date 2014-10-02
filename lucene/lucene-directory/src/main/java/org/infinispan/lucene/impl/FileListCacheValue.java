package org.infinispan.lucene.impl;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.lucene.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Maintains a Set of filenames contained in the index. Does not implement Set for simplicity, and does internal locking
 * to provide a safe Externalizer.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
@ThreadSafe
public final class FileListCacheValue implements DeltaAware {

   private final Set<String> filenames = new HashSet<>();
   private FileListCacheValueDelta fileListValueDelta = new FileListCacheValueDelta();
   private final Lock writeLock;
   private final Lock readLock;

   /**
    * Constructs a new empty set of filenames
    */
   public FileListCacheValue() {
      ReadWriteLock namesLock = new ReentrantReadWriteLock();
      writeLock = namesLock.writeLock();
      readLock = namesLock.readLock();
   }

   /**
    * Initializes a new instance storing the passed values.
    * @param listAll the strings to store.
    */
   public FileListCacheValue(String[] listAll) {
      this();
      Collections.addAll(filenames, listAll);
   }

   protected void apply(List<Operation> operations) {
      writeLock.lock();
      try {
         for (Operation operation : operations) {
            operation.apply(filenames);
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Removes the filename from the set if it exists
    * @param fileName
    * @return true if the set was mutated
    */
   public boolean remove(String fileName) {
      writeLock.lock();
      try {
         boolean removed = filenames.remove(fileName);
         if (removed) {
            fileListValueDelta.removeOperation(fileName);
         }
         return removed;
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Adds the filename from the set if it exists
    * @param fileName
    * @return true if the set was mutated
    */
   public boolean add(String fileName) {
      writeLock.lock();
      try {
         boolean added = filenames.add(fileName);
         if (added) {
            fileListValueDelta.addOperation(fileName);
         }
         return added;
      } finally {
         writeLock.unlock();
      }
   }

   public boolean addAndRemove(String toAdd, String toRemove) {
      writeLock.lock();
      try {
         boolean doneAdd = filenames.add(toAdd);
         boolean doneRemove = filenames.remove(toRemove);
         if (doneAdd) {
            fileListValueDelta.addOperation(toAdd);
         }
         if (doneRemove) {
            fileListValueDelta.removeOperation(toRemove);
         }
         return doneAdd || doneRemove;
      } finally {
         writeLock.unlock();
      }
   }

   public String[] toArray() {
      readLock.lock();
      try {
         return filenames.toArray(new String[filenames.size()]);
      } finally {
         readLock.unlock();
      }
   }

   public boolean contains(String fileName) {
      readLock.lock();
      try {
         return filenames.contains(fileName);
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public int hashCode() {
      readLock.lock();
      try {
         return filenames.hashCode();
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (FileListCacheValue.class != obj.getClass())
         return false;
      final FileListCacheValue other = (FileListCacheValue) obj;
      final HashSet<String> copyFromOther;
      other.readLock.lock();
      try {
         copyFromOther = new HashSet<>(other.filenames);
      } finally {
         other.readLock.unlock();
      }
      readLock.lock();
      try {
         return (filenames.equals(copyFromOther));
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public String toString() {
      readLock.lock();
      try {
         return "FileListCacheValue [filenames=" + filenames + "]";
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public FileListCacheValueDelta delta() {
      readLock.lock();
      try {
         FileListCacheValueDelta toReturn = fileListValueDelta;
         fileListValueDelta = new FileListCacheValueDelta();
         return toReturn;
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public void commit() {
      fileListValueDelta.discardOps();
   }

   public static final class Externalizer extends AbstractExternalizer<FileListCacheValue> {

      @Override
      public void writeObject(final ObjectOutput output, final FileListCacheValue key) throws IOException {
         key.readLock.lock();
         try {
            UnsignedNumeric.writeUnsignedInt(output, key.filenames.size());
            for (String name : key.filenames) {
               output.writeUTF(name);
            }
         } finally {
            key.readLock.unlock();
         }
      }

      @Override
      public FileListCacheValue readObject(final ObjectInput input) throws IOException {
         int size = UnsignedNumeric.readUnsignedInt(input);
         String[] names = new String[size];
         for (int i = 0; i < size; i++) {
            names[i] = input.readUTF();
         }
         return new FileListCacheValue(names);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_LIST_CACHE_VALUE;
      }

      @Override
      public Set<Class<? extends FileListCacheValue>> getTypeClasses() {
         return Util.<Class<? extends FileListCacheValue>>asSet(FileListCacheValue.class);
      }

   }

}
