package org.infinispan.lucene.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.protostream.annotations.ProtoField;

import net.jcip.annotations.ThreadSafe;

/**
 * Maintains a Set of filenames contained in the index. Does not implement Set for simplicity, and does internal locking
 * to provide a safe Externalizer.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
@ThreadSafe
public final class FileListCacheValue {

   private final Set<String> filenames = new HashSet<>();
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

   @ProtoField(number = 1, collectionImplementation = HashSet.class, name = "file_names")
   Set<String> getFileNames() {
      readLock.lock();
      try {
         return new HashSet<>(filenames);
      } finally {
         readLock.unlock();
      }
   }

   /**
    * Setter method for protostream, writeLock is not strictly necessary as it should not be
    * possible for this method to be called before protostream returns the initialized method.
    */
   void setFileNames(Set<String> names) {
      writeLock.lock();
      try {
         this.filenames.addAll(names);
      } finally {
         writeLock.unlock();
      }
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
         return filenames.remove(fileName);
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
         return filenames.add(fileName);
      } finally {
         writeLock.unlock();
      }
   }

   public boolean addAndRemove(String toAdd, String toRemove) {
      writeLock.lock();
      try {
         boolean doneAdd = filenames.add(toAdd);
         boolean doneRemove = filenames.remove(toRemove);
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
}
