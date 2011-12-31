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

import org.infinispan.Cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Entry point for GridFile and GridInputStream / GridOutputStream
 *
 * @author Bela Ban
 */
public class GridFilesystem {
   protected final Cache<String, byte[]> data;
   protected final Cache<String, GridFile.Metadata> metadata;
   protected final int default_chunk_size;

   /**
    * Creates an instance. The data and metadata caches should already have been setup and started
    *
    * @param data
    * @param metadata
    * @param default_chunk_size
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

   public File getFile(String pathname) {
      return getFile(pathname, default_chunk_size);
   }

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

   public OutputStream getOutput(String pathname) throws IOException {
      return getOutput(pathname, false, default_chunk_size);
   }

   public OutputStream getOutput(String pathname, boolean append) throws IOException {
      return getOutput(pathname, append, default_chunk_size);
   }

   public OutputStream getOutput(String pathname, boolean append, int chunk_size) throws IOException {
      GridFile file = (GridFile) getFile(pathname, chunk_size);
      if (file.isDirectory()) {
         throw new FileNotFoundException("Cannot write to directory (" + pathname + ")");
      }
      if (!file.createNewFile())
         throw new IOException("creation of " + pathname + " failed");

      return new GridOutputStream(file, append, data);
   }

   public OutputStream getOutput(GridFile file) throws IOException {
      if (file.isDirectory()) {
         throw new FileNotFoundException("Cannot write to directory (" + file + ")");
      }
      if (!file.createNewFile())
         throw new IOException("creation of " + file + " failed");
      return new GridOutputStream(file, false, data);
   }


   public InputStream getInput(String pathname) throws FileNotFoundException {
      GridFile file = (GridFile) getFile(pathname);
      if (!file.exists())
         throw new FileNotFoundException(pathname);
      if (file.isDirectory()) {
         throw new FileNotFoundException("Cannot read from directory (" + file + ")");
      }
      return new GridInputStream(file, data);
   }

   public InputStream getInput(File pathname) throws FileNotFoundException {
      return pathname != null ? getInput(pathname.getPath()) : null;
   }


   public void remove(String path, boolean synchronous) {
      if (path == null)
         return;
      GridFile.Metadata md = metadata.get(path);
      if (md == null)
         return;
      int num_chunks = md.getLength() / md.getChunkSize() + 1;
      for (int i = 0; i < num_chunks; i++)
         data.remove(path + ".#" + i, synchronous);
   }
}
