package org.infinispan.lucene.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import net.jcip.annotations.ThreadSafe;

/**
 * Maintains a Set of file names contained in the index. Does not implement Set for simplicity, and does internal
 * locking to provide a safe Externalizer.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
@ThreadSafe
@ProtoTypeId(ProtoStreamTypeIds.FILE_LIST_CACHE_VALUE)
public final class FileListCacheValue {

   private final Set<String> fileNames;
   private final Lock writeLock;
   private final Lock readLock;

   /**
    * Constructs a new empty set of file names.
    */
   FileListCacheValue() {
      this(new HashSet<>());
   }

   @ProtoFactory
   FileListCacheValue(Set<String> fileNames) {
      ReadWriteLock namesLock = new ReentrantReadWriteLock();
      this.fileNames = fileNames;
      this.writeLock = namesLock.writeLock();
      this.readLock = namesLock.readLock();
   }

   @ProtoField(number = 1)
   Set<String> getFileNames() {
      readLock.lock();
      try {
         return new HashSet<>(fileNames);
      } finally {
         readLock.unlock();
      }
   }

   /**
    * Initializes a new instance storing the passed values.
    *
    * @param listAll the strings to store.
    */
   public FileListCacheValue(String[] listAll) {
      this();
      Collections.addAll(fileNames, listAll);
   }

   protected void apply(List<Operation> operations) {
      writeLock.lock();
      try {
         for (Operation operation : operations) {
            operation.apply(fileNames);
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Removes the filename from the set if it exists
    *
    * @param fileName
    * @return true if the set was mutated
    */
   public boolean remove(String fileName) {
      writeLock.lock();
      try {
         return fileNames.remove(fileName);
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Adds the filename from the set if it exists
    *
    * @param fileName
    * @return true if the set was mutated
    */
   public boolean add(String fileName) {
      writeLock.lock();
      try {
         return fileNames.add(fileName);
      } finally {
         writeLock.unlock();
      }
   }

   public boolean addAndRemove(String toAdd, String toRemove) {
      writeLock.lock();
      try {
         boolean doneAdd = fileNames.add(toAdd);
         boolean doneRemove = fileNames.remove(toRemove);
         return doneAdd || doneRemove;
      } finally {
         writeLock.unlock();
      }
   }

   public String[] toArray() {
      readLock.lock();
      try {
         return fileNames.toArray(new String[0]);
      } finally {
         readLock.unlock();
      }
   }

   public boolean contains(String fileName) {
      readLock.lock();
      try {
         return fileNames.contains(fileName);
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public int hashCode() {
      readLock.lock();
      try {
         return fileNames.hashCode();
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null || FileListCacheValue.class != obj.getClass())
         return false;
      final FileListCacheValue other = (FileListCacheValue) obj;
      final Set<String> copyFromOther = other.getFileNames();
      readLock.lock();
      try {
         return fileNames.equals(copyFromOther);
      } finally {
         readLock.unlock();
      }
   }

   @Override
   public String toString() {
      return "FileListCacheValue{fileNames=" + getFileNames() + "}";
   }
}
