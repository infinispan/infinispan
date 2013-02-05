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
package org.infinispan.loaders.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
* A filesystem-based implementation of a {@link CacheStore}. This file store stores cache values in
* multiple {@link FileStore}. There is only one active file at a time where data is written
* Data is written in append-only way, thus any operation is appended at the end of the file
* There is a  compactor thread who verifies files eligible for compaction and rewrites the valid data from these files to the current active file
* keys and file positions are kept in memory.
* @author Patrick Azogni
*/

@CacheLoaderMetadata(configurationClass = FileCacheStoreRedesignConfig.class)
public class FileCacheStoreRedesign extends AbstractCacheStore {

   private FileCacheStoreRedesignConfig config;

   private BufferHandler bufferHandler;
   private Map<Integer, FileEntry> entries = ConcurrentMapFactory.makeConcurrentMap();
   private SortedMap<String, FileStore> fileList;
   private SortedSet<FileStore> unUsedFileList = new TreeSet<FileStore>();

   private final EntryAddress addressHandler = new EntryAddress();

   private String location;
   private volatile String activeFileName;

   static final String INDEX_FILENAME = "index";
   static final Log log = LogFactory.getLog(FileCacheStoreRedesign.class);

   private Compactor compactor;

   private NumericNamedFilesFilter NUMERIC_NAMED_FILES_FILTER;

   /** {@inheritDoc} */
   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (FileCacheStoreRedesignConfig) config;
   }

   /** {@inheritDoc} */
   @Override
   public void start() throws CacheLoaderException {
      super.start();
      try{
         log.info("Initializing");

         if (config == null) {
            throw new CacheLoaderException("Null config. Possible reason is not calling super.init(...)");
         }

         bufferHandler = new BufferHandler(getMarshaller());
         final Map<Integer, FileEntry> entryMap = new HashMap<Integer, FileEntry>();
         entries = Collections.synchronizedMap(entryMap);

         final SortedMap<String, FileStore> fileListMap = new TreeMap<String, FileStore>();
         fileList = Collections.synchronizedSortedMap(fileListMap);
         
         preload();
         
         compactor = new Compactor();
         compactor.start();
      }
      catch (Exception e){
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }

   }

   private void preload() throws Exception {
      
      try {
         location = config.getLocation();
         if (location == null || location.trim().length() == 0)
            location = "Infinispan-FileCacheStore";
         location += File.separator+cache.getName();
         File dir = new File(location);
         if (!dir.exists() && !dir.mkdirs())
            throw new ConfigurationException("Directory " + dir.getAbsolutePath()
                  + " does not exist and cannot be created!");
   
         if (dir.isFile())
            throw new ConfigurationException(dir.getAbsolutePath()+ " Is not a directory");
   
         NUMERIC_NAMED_FILES_FILTER = new NumericNamedFilesFilter(config.getNumberFiles());
   
         String[] files = dir.list(NUMERIC_NAMED_FILES_FILTER);
         for (String fileName:files){
   
            FileStore fileStore = new FileStore(fileName, location, config.getMaxSizePerFile() * 1024 * 1024);
            if (fileStore.init()) {
               if (!fileStore.isReadOnly()){
                  activeFileName = fileName;
               }
               fileList.put(fileName, fileStore);
            }
            else {
               unUsedFileList.add(fileStore);
            }
   
         }
   
         if (activeFileName == null) {
            FileStore df = createNewFile();
            df.setMode(FileStore.READ_WRITE);
            fileList.put(df.getFilename(), df);
            activeFileName = df.getFilename();
         }
   
         loadStoredEntries ();
         loadEntries();
         setFileStoreLoad();
      }
      catch (Exception e) {
         throw new CacheLoaderException(e);
      }
      
   }
   
   /**
    * Loads persisted entries into memory
    */
   private void loadStoredEntries () throws Exception {

      try {
         File f = new File(location, INDEX_FILENAME);
         RandomAccessFile raf = new RandomAccessFile(f, "rw");
         FileChannel fc = raf.getChannel();

         ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
         fc.read(bb, 0);
         bb.flip();
         while (bb.hasRemaining()){
            int hash = bb.getInt();
            long addr = bb.getLong();
            long expiryTime = bb.getLong();
            FileEntry fe = new FileEntry(addr, expiryTime);
            entries.put(hash, fe);
         }

         Util.close(raf);
         Util.close(fc);
      }
      catch (Exception e){
         throw new CacheLoaderException(e);
      }

   }
   
   /**
    * Parse active file and loads cache entries in memory
    */
   private void loadEntries () throws Exception {
      FileStore fileStore = getFileStore(activeFileName);
      long size = fileStore.getSize();
      long filePos = FileStore.HEADER_LENGTH;
      int keyHash, dataLen;
      long expiryTime;

      while (filePos < size) {

         ByteBuffer buf = fileStore.readData(filePos, FileStore.MINIMUM_LENGTH);
         int flag = buf.getInt();
         if (flag == FileStore.DELETE_FLAG) {
            dataLen = buf.getInt();
            keyHash = buf.getInt();
            entries.remove(keyHash);
         }
         else {
            dataLen = buf.getInt();
            keyHash = buf.getInt();
            expiryTime = buf.getLong();

            long addr = addressHandler.composeAddress((int)filePos, activeFileName, dataLen);
            FileEntry fe = new FileEntry(addr, expiryTime);
            entries.put(keyHash, fe);
         }
         
         filePos += dataLen;
      }
   }

   /**
    * Creates new cache file to write data
    */
   private synchronized FileStore createNewFile () throws CacheLoaderException {

      try {
         FileStore df;
         if (!unUsedFileList.isEmpty()) {
            df = unUsedFileList.first();
            unUsedFileList.remove(df);
            return df;
         }

         String fileName;
         if (!fileList.isEmpty()) {
            String lastfilename = fileList.lastKey();
            fileName = new Integer(Integer.parseInt(lastfilename) + 1).toString();
         }
         else {
            fileName = "1";
         }

         return new FileStore(fileName, location, config.getMaxSizePerFile() * 1024 * 1024);
      }
      catch (Exception e) {
         throw new CacheLoaderException(e);
      }

   }

   /**
    * Defines the total length of data contained in each cache file
    */
   private void setFileStoreLoad () throws CacheLoaderException {

      try {
         FileEntry entry;
         for (Iterator<FileEntry> it = entries.values().iterator(); it.hasNext();) {
            entry = it.next();
            int dataLen = addressHandler.getDataLen(entry.address);
            String filename = addressHandler.getFileName(entry.address);
            int size = dataLen;
            FileStore fileStore = fileList.get(filename);
            if (fileStore != null)
               fileStore.incrementDataSize(size);
         }

      }
      catch (Exception e){
         throw new CacheLoaderException(e);
      }

   }

   /**
    * Return a cache file
    * @Param name - the cache file name
    */
   private FileStore getFileStore(String name) {
      return fileList.get(name);
   }

   /** {@inheritDoc} */
   @Override
   public void stop() throws CacheLoaderException {
      super.stop();

      try {
         compactor.stop();
      }
      catch (Exception e){
         throw new CacheLoaderException(e);
      }

   }

   /** {@inheritDoc} */
   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude)
         throws CacheLoaderException {

      Set<Object> keyList = new HashSet<Object>();
      Set<Integer> keyHashToExclude = new HashSet<Integer>();
      if (keysToExclude != null)
         for (Object key:keysToExclude){
            keyHashToExclude.add(getHashCode(key));
         }

      Set<Integer> keyHashList = loadAllKeysHash(keyHashToExclude);
      for (Integer keyHash:keyHashList){
         long addr = entries.get(keyHash).address;
         ByteBuffer bb = loadData(addr);
         if (bb != null) {
            Object key = bufferHandler.getKeyFromBuffer(bb);
            keyList.add(key);
         }
      }

      return keyList;
   }

   private Set<Integer> loadAllKeysHash(Set<Integer> keysHashToExclude)
         throws CacheLoaderException {

      Set<Integer> result;
      synchronized (entries) {
         result = new HashSet<Integer>(entries.keySet());
      }
      if (keysHashToExclude != null)
         result.removeAll(keysHashToExclude);
      return result;
   }

   /** {@inheritDoc} */
   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return FileCacheStoreRedesignConfig.class;
   }

   /** {@inheritDoc} */
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      long now = System.currentTimeMillis();
      synchronized (entries) {
         for (Iterator<Entry<Integer, FileEntry>> it = entries.entrySet().iterator(); it.hasNext();) {
            Entry<Integer, FileEntry> entry = it.next();
            FileEntry fe = entry.getValue();

            if (fe.isExpired(now)) {
               int keyHash = entry.getKey();
               removeInternal(keyHash, entry.getValue());
               it.remove();
               try {
               } catch (Exception e) {
                  throw new CacheLoaderException(e);
               }
            }
         }
      }

   }

   /**
    * Write Data to current active file. If active file is full, a new file is created
    */
   private synchronized long writeData (ByteBuffer buf) throws CacheLoaderException {

      try {
         final FileStore fileStore = getFileStore(activeFileName);
         long offset = fileStore.appendData(buf);
         if (offset == -1) {
            addNewActiveFile ();
            offset = writeData(buf);
         }
         return offset;
      }
      catch (Exception e) {
         throw new CacheLoaderException(e);
      }

   }

   /**
    * Adds new active read write file
    */
   private void addNewActiveFile () throws CacheLoaderException {

      try {
         FileStore df = createNewFile();
         df.setMode(FileStore.READ_WRITE);
         if (df != null) {
            Set<Entry<Integer, FileEntry>> entrySet = entries.entrySet();
            persistFileEntries (entrySet);
            
            fileList.put(df.getFilename(), df);
            String oldActiveFileName = activeFileName;
            activeFileName = df.getFilename();
            fileList.get(oldActiveFileName).setMode(FileStore.READ_ONLY);
         }
      }
      catch (Exception e) {
         throw new CacheLoaderException();
      }

   }

   /**
    * Persist in memory entries to file. This helps for fast loading during starting of the cache
    */
   private void persistFileEntries (Set<Entry<Integer, FileEntry>> entrySet) throws CacheLoaderException {

      try {
         File index = new File(location, INDEX_FILENAME);
         RandomAccessFile raf = new RandomAccessFile(index, "rw");
         FileChannel channel = raf.getChannel();
 
         if (!entrySet.isEmpty()){
            // Allocate the necessary size to persist all entries. (Each Entry Needing 20bytes)
            ByteBuffer bb = ByteBuffer.allocate(entries.size() * 20);
            for (Map.Entry<Integer, FileEntry> entry:entrySet) {
               bb.putInt(entry.getKey());
               bb.putLong(entry.getValue().address);
               bb.putLong(entry.getValue().expiryTime);
            }
            bb.flip();
            channel.truncate(0);
            channel.write(bb);
         }
         else {
            // If there is no entry, just clear the file
            channel.truncate(0);
         }

         channel.force(true);
         Util.close(channel);
         Util.close(raf);

      }
      catch (Exception e){
         throw new CacheLoaderException();
      }

   }

   /**
    * Makes sure that files opened are actually named as numbers (ignore all other files)
    */
   private static class NumericNamedFilesFilter implements FilenameFilter {

      private int numberFiles;
      public NumericNamedFilesFilter(int numberFiles){
         this.numberFiles = numberFiles;
      }

      @Override
      public boolean accept(File dir, String name) {
         try {
            int nameInt = Integer.parseInt(name);
            if ((nameInt < 0) || (nameInt > numberFiles)) {
               return false;
            }
         }
         catch (NumberFormatException e){
            return false;
         }
         return true;
      }
   }

   /** {@inheritDoc} */
   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {

      try {

         int keyHash = getHashCode(entry.getKey());

         storeInternal(entry, keyHash);

      } catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }

   }

   private void storeInternal(InternalCacheEntry entry, int keyHash) throws CacheLoaderException {

      try {
         byte[] dataByte = getMarshaller().objectToByteBuffer(entry.toInternalCacheValue());
         byte[] keyByte = getMarshaller().objectToByteBuffer(entry.getKey());

         ByteBuffer buf = bufferHandler.compose(FileStore.INSERT_FLAG, keyHash, keyByte, dataByte, entry.getExpiryTime());

         int offset = (int)writeData(buf);
         long address = addressHandler.composeAddress(offset, activeFileName, buf.array().length);
         FileEntry fe = new FileEntry(address, entry.getExpiryTime());
         FileEntry old_entry = entries.put(keyHash, fe);
         if (old_entry != null){
            decrementDataSize(fe);
         }
         
         getFileStore(activeFileName).incrementDataSize(buf.array().length);
         
      } catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }
   }
   
   /**
    * This method decrease the data size value from a filestore after a delete or an update
    */
   private void decrementDataSize(FileEntry fe) {
      long addr = fe.address;
      String filename = addressHandler.getFileName(addr);
      int dataLen = addressHandler.getDataLen(addr);
      getFileStore(filename).decrementDataSize(dataLen);
   }
   
   /**
    * Returns the object hash code
    */
   private int getHashCode(Object key) {
      return key.hashCode();
   }

   /** {@inheritDoc} */
   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
   }

   /** {@inheritDoc} */
   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
   }

   /** {@inheritDoc} */
   @Override
   public void clear() throws CacheLoaderException {
      try {
         entries.clear();
         persistFileEntries(entries.entrySet());
         synchronized (fileList) {
            for (FileStore fileStore : fileList.values()) {
               fileStore.clear();
            }
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   /** {@inheritDoc} */
   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      try {
         int keyHash = getHashCode(key);
         FileEntry fe = entries.remove(keyHash);
         if (fe != null) {
            
            return removeInternal(keyHash, fe);
         }

         return false;
      } catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }
   }

   private boolean removeInternal (int keyHash, FileEntry fe) throws CacheLoaderException {
      try {
         ByteBuffer bb = bufferHandler.compose(FileStore.DELETE_FLAG, keyHash, null, null, 0L);
         writeData (bb);
         decrementDataSize(fe);
         return true;
      } catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }
   }

   /** {@inheritDoc} */
   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {

      try {
         int keyHash = getHashCode(key);
         FileEntry fe = entries.get(keyHash);
         if (fe != null){
            
            long addr = fe.address;
            ByteBuffer bb = loadData(addr);
            if (bb != null) {
               InternalCacheValue value = bufferHandler.getValueFromBuffer(bb);
               InternalCacheEntry entry = value.toInternalCacheEntry(key);

               return entry;
            }
         }

         return null;

      }

      catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Loads data from a file store
    * @param addr - address of the data (Offset, filename, length)
    * @return byte buffer wrapping the actual data
    */
   private ByteBuffer loadData(long addr) throws CacheLoaderException {

      try{
         int offset = addressHandler.getOffset(addr);
         int dataLen = addressHandler.getDataLen(addr);
         String filename = addressHandler.getFileName(addr);
         FileStore fileStore = getFileStore(filename);
         if (fileStore != null){
            final ByteBuffer bb = fileStore.readData(offset, dataLen);
            return bb;
         }

         return null;
      }
      catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }
   }

   /** {@inheritDoc} */
   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(Integer.MAX_VALUE);
   }

   /** {@inheritDoc} */
   @Override
   public Set<InternalCacheEntry> load(int numEntries)
         throws CacheLoaderException {

      long now = System.currentTimeMillis();
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
      synchronized (entries) {
         for (Iterator<Entry<Integer, FileEntry>> it = entries.entrySet().iterator(); it.hasNext();) {
            Entry<Integer, FileEntry> entry = it.next();
            FileEntry fe = entry.getValue();
            if (!fe.isExpired(now)){
               ByteBuffer bb = loadData(fe.address);
               if (bb != null){
                  Object key = bufferHandler.getKeyFromBuffer(bb);
                  InternalCacheEntry ice = bufferHandler.getValueFromBuffer(bb).toInternalCacheEntry(key);
                  result.add(ice);
                  if (result.size() > numEntries)
                     return result;
               }
            }
            else {
               it.remove();
               removeInternal(entry.getKey(), fe);
            }
         }
      }
      return result;
   }

   private static class FileEntry {

      private volatile long address;
      private long expiryTime = -1;

      private FileEntry (long address, long expiryTime) {
         this.address = address;
         this.expiryTime = expiryTime;
      }

      private boolean isExpired(long now) {
         return expiryTime > 0 && expiryTime < now;
      }

   }

   public class Compactor implements Runnable {

      private ExecutorService executor;
      private volatile boolean compact;
      private long timeout = 10;
      private BlockingQueue<String> compactQueue = new LinkedBlockingQueue<String>();
      /**
       * @param args
       */

      final void start() {
         compact = true;
         executor = Executors.newSingleThreadExecutor();
         executor.execute(this);
      }
      
      private void CompactorAnalyser() {
         for (FileStore fileStore : fileList.values()) {
            if ((fileStore.getLoad() <= config.getLoadThreshold()) && fileStore.isReadOnly()) {
               try {
                  compactQueue.put(fileStore.getFilename());
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
         }
      }

      public void run () {

         while (compact) {
            try {
               CompactorAnalyser();
               String fileToCompact = compactQueue.poll();
               if (fileToCompact != null) {
                  long now = System.currentTimeMillis();
                  synchronized (entries) {

                     for (Iterator<Entry<Integer, FileEntry>> it = entries.entrySet().iterator(); it.hasNext();) {

                        Entry<Integer, FileEntry> entry = it.next();
                        if (entry.getValue().isExpired(now)){
                           it.remove();
                           continue;
                        }
                        long addr = entry.getValue().address;
                        String filename = addressHandler.getFileName(addr) + "";

                        if (filename.equals(fileToCompact)) {
                           ByteBuffer bb = loadData(addr);
                           if (bb != null) {
                              int newOffset = (int)writeData(bb);
                              long newAddr = addressHandler.composeAddress(newOffset, activeFileName, bb.capacity());
                              FileEntry fe = new FileEntry(newAddr, entry.getValue().expiryTime);
                              entry.setValue(fe);
                           }
                        }
                     }
                  }
                  
                  FileStore df = getFileStore(fileToCompact);
                  fileList.remove(fileToCompact);
                  df.clear();
                  unUsedFileList.add(df);
               }
               else {
                  Thread.sleep(5000);
               }
            }
            catch (Exception e) {
               e.printStackTrace();
            }
         }

      }

      public void stop () {

         compact = false;
         try {
            executor.shutdown();
            executor.awaitTermination(timeout, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }

      }

   }
}
