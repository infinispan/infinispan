/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.io;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.infinispan.context.Flag.FORCE_ASYNCHRONOUS;
import static org.infinispan.context.Flag.FORCE_SYNCHRONOUS;

/**
 * Entry point for GridFile and GridInputStream / GridOutputStream
 *
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridFilesystem {
   protected final Cache<String, byte[]> data;
   protected final Cache<String, GridFile.Metadata> metadata;
   protected final int defaultChunkSize;

   /**
    * Creates an instance. The data and metadata caches should already have been setup and started
    *
    * @param data the cache where the actual file contents are stored
    * @param metadata the cache where file meta-data is stored
    * @param defaultChunkSize the default size of the file chunks
    */
   public GridFilesystem(Cache<String, byte[]> data, Cache<String, GridFile.Metadata> metadata, int defaultChunkSize) {
      this.data = data;
      this.metadata = metadata;
      this.defaultChunkSize = defaultChunkSize;
   }

   public GridFilesystem(Cache<String, byte[]> data, Cache<String, GridFile.Metadata> metadata) {
      this(data, metadata, 8000);
   }

   /**
    * Returns the file denoted by pathname.
    * @param pathname the full path of the requested file
    * @return the File stored at pathname
    */
   public File getFile(String pathname) {
      return getFile(pathname, defaultChunkSize);
   }

   /**
    * Returns the file denoted by pathname. If the file does not yet exist, it is initialized with the given chunkSize.
    * However, if the file at pathname already exists, the chunkSize parameter is ignored and the file's actual
    * chunkSize is used.
    * @param pathname the full path of the requested file
    * @param chunkSize the size of the file's chunks. This parameter is only used for non-existing files.
    * @return the File stored at pathname
    */
   public File getFile(String pathname, int chunkSize) {
      return new GridFile(pathname, metadata, chunkSize, this);
   }

   public File getFile(String parent, String child) {
      return getFile(parent, child, defaultChunkSize);
   }

   public File getFile(String parent, String child, int chunkSize) {
      return new GridFile(parent, child, metadata, chunkSize, this);
   }

   public File getFile(File parent, String child) {
      return getFile(parent, child, defaultChunkSize);
   }

   public File getFile(File parent, String child, int chunkSize) {
      return new GridFile(parent, child, metadata, chunkSize, this);
   }

   /**
    * Opens an OutputStream for writing to the file denoted by pathname. If a file at pathname already exists, writing
    * to the returned OutputStream will overwrite the contents of the file.
    * @param pathname the path to write to
    * @return an OutputStream for writing to the file at pathname
    * @throws IOException if an error occurs
    */
   public OutputStream getOutput(String pathname) throws IOException {
      return getOutput(pathname, false, defaultChunkSize);
   }

   /**
    * Opens an OutputStream for writing to the file denoted by pathname. The OutputStream can either overwrite the
    * existing file or append to it.
    * @param pathname the path to write to
    * @param append if true, the bytes written to the OutputStream will be appended to the end of the file. If false,
    *               the bytes will overwrite the original contents.
    * @return an OutputStream for writing to the file at pathname
    * @throws IOException if an error occurs
    */
   public OutputStream getOutput(String pathname, boolean append) throws IOException {
      return getOutput(pathname, append, defaultChunkSize);
   }

   /**
    * Opens an OutputStream for writing to the file denoted by pathname.
    * @param pathname the file to write to
    * @param append if true, the bytes written to the OutputStream will be appended to the end of the file
    * @param chunkSize the size of the file's chunks. This parameter is honored only when the file at pathname does
    *        not yet exist. If the file already exists, the file's own chunkSize has precedence.
    * @return the OutputStream for writing to the file
    * @throws IOException if the file is a directory, cannot be created or some other error occurs
    */
   public OutputStream getOutput(String pathname, boolean append, int chunkSize) throws IOException {
      GridFile file = (GridFile) getFile(pathname, chunkSize);
      checkIsNotDirectory(file);
      createIfNeeded(file);
      return new GridOutputStream(file, append, data);
   }

   /**
    * Opens an OutputStream for writing to the given file.
    * @param file the file to write to
    * @return an OutputStream for writing to the file
    * @throws IOException if an error occurs
    */
   public OutputStream getOutput(GridFile file) throws IOException {
      checkIsNotDirectory(file);
      createIfNeeded(file);
      return new GridOutputStream(file, false, data);
   }

   private void checkIsNotDirectory(GridFile file) throws FileNotFoundException {
      if (file.isDirectory()) {
         throw new FileNotFoundException(file + " is a directory.");
      }
   }

   private void createIfNeeded(GridFile file) throws IOException {
      if (!file.exists() && !file.createNewFile())
         throw new IOException("creation of " + file + " failed");
   }

   /**
    * Opens an InputStream for reading from the file denoted by pathname.
    * @param pathname the full path of the file to read from
    * @return an InputStream for reading from the file
    * @throws FileNotFoundException if the file does not exist or is a directory
    */
   public InputStream getInput(String pathname) throws FileNotFoundException {
      GridFile file = (GridFile) getFile(pathname);
      if (!file.exists())
         throw new FileNotFoundException(pathname);
      checkIsNotDirectory(file);
      return new GridInputStream(file, data);
   }

   /**
    * Opens an InputStream for reading from the given file.
    * @param file the file to open for reading
    * @return an InputStream for reading from the file
    * @throws FileNotFoundException if the file does not exist or is a directory
    */
   public InputStream getInput(File file) throws FileNotFoundException {
      return file != null ? getInput(file.getPath()) : null;
   }

   /**
    * Removes the file denoted by path. This operation can either be executed synchronously or asynchronously.
    * @param path the file to remove
    * @param synchronous if true, the method will return only after the file has actually been removed;
    *                    if false, the method will return immediately and the file will be removed asynchronously.
    */
   void remove(String path, boolean synchronous) {
      if (path == null)
         return;
      GridFile.Metadata md = metadata.get(path);
      if (md == null)
         return;

      Flag flag = synchronous ? FORCE_SYNCHRONOUS : FORCE_ASYNCHRONOUS;
      AdvancedCache<String,byte[]> advancedCache = data.getAdvancedCache().withFlags(flag);
      int numChunks = md.getLength() / md.getChunkSize() + 1;
      for (int i = 0; i < numChunks; i++)
         advancedCache.remove(path + ".#" + i);
   }
}
