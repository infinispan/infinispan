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
   protected final int default_chunk_size;

   /**
    * Creates an instance. The data and metadata caches should already have been setup and started
    *
    * @param data the cache where the actual file contents are stored
    * @param metadata the cache where file meta-data is stored
    * @param default_chunk_size the default size of the file chunks
    */
   public GridFilesystem(Cache<String, byte[]> data, Cache<String, GridFile.Metadata> metadata,
                         int default_chunk_size) {
      this.data = data;
      this.metadata = metadata;
      this.default_chunk_size = default_chunk_size;
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
      return getFile(pathname, default_chunk_size);
   }

   /**
    * Returns the file denoted by pathname. If the file does not yet exist, it is initialized with the given chunk_size.
    * However, if the file at pathname already exists, the chunk_size parameter is ignored and the file's actual
    * chunk_size is used.
    * @param pathname the full path of the requested file
    * @param chunk_size the size of the file's chunks. This parameter is only used for non-existing files.
    * @return the File stored at pathname
    */
   public File getFile(String pathname, int chunk_size) {
      return new GridFile(pathname, metadata, chunk_size, this);
   }

   public File getFile(String parent, String child) {
      return getFile(parent, child, default_chunk_size);
   }

   public File getFile(String parent, String child, int chunk_size) {
      return new GridFile(parent, child, metadata, chunk_size, this);
   }

   public File getFile(File parent, String child) {
      return getFile(parent, child, default_chunk_size);
   }

   public File getFile(File parent, String child, int chunk_size) {
      return new GridFile(parent, child, metadata, chunk_size, this);
   }

   /**
    * Opens an OutputStream for writing to the file denoted by pathname. If a file at pathname already exists, writing
    * to the returned OutputStream will overwrite the contents of the file.
    * @param pathname the path to write to
    * @return an OutputStream for writing to the file at pathname
    * @throws IOException if an error occurs
    */
   public OutputStream getOutput(String pathname) throws IOException {
      return getOutput(pathname, false, default_chunk_size);
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
      return getOutput(pathname, append, default_chunk_size);
   }

   /**
    * Opens an OutputStream for writing to the file denoted by pathname.
    * @param pathname the file to write to
    * @param append if true, the bytes written to the OutputStream will be appended to the end of the file
    * @param chunk_size the size of the file's chunks. This parameter is honored only when the file at pathname does
    *        not yet exist. If the file already exists, the file's own chunkSize has precedence.
    * @return the OutputStream for writing to the file
    * @throws IOException if the file is a directory, cannot be created or some other error occurs
    */
   public OutputStream getOutput(String pathname, boolean append, int chunk_size) throws IOException {
      GridFile file = (GridFile) getFile(pathname, chunk_size);
      if (file.isDirectory()) {
         throw new FileNotFoundException("Cannot write to directory (" + pathname + ")");
      }
      if (!file.exists() && !file.createNewFile())
         throw new IOException("creation of " + pathname + " failed");

      return new GridOutputStream(file, append, data);
   }

   /**
    * Opens an OutputStream for writing to the given file.
    * @param file the file to write to
    * @return an OutputStream for writing to the file
    * @throws IOException if an error occurs
    */
   public OutputStream getOutput(GridFile file) throws IOException {
      if (file.isDirectory()) {
         throw new FileNotFoundException("Cannot write to directory (" + file + ")");
      }
      if (!file.exists() && !file.createNewFile())
         throw new IOException("creation of " + file + " failed");
      return new GridOutputStream(file, false, data);
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
      if (file.isDirectory()) {
         throw new FileNotFoundException("Cannot read from directory (" + file + ")");
      }
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
