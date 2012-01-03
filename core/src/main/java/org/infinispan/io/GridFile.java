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
import org.jgroups.util.Util;

import java.io.Externalizable;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Set;

import static java.lang.String.format;
import static org.infinispan.context.Flag.FORCE_SYNCHRONOUS;

/**
 * Subclass of File to iterate through directories and files in a grid
 *
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridFile extends File {
   private static final long serialVersionUID = -6729548421029004260L;
   private static final Metadata ROOT_DIR_METADATA = new Metadata(0, 0, 0, Metadata.DIR);
   private static final char SEPARATOR_CHAR = '/';
   private static final String SEPARATOR = "" + SEPARATOR_CHAR;
   private final AdvancedCache<String, Metadata> metadataCache;
   private final GridFilesystem fs;
   private final String name;
   private int chunk_size;

   GridFile(String pathname, Cache<String, Metadata> metadataCache, int chunk_size, GridFilesystem fs) {
      super(pathname);
      this.fs = fs;
      this.name = trim(pathname);
      this.metadataCache = metadataCache.getAdvancedCache();
      this.chunk_size = chunk_size;
      initChunkSizeFromMetadata();
   }

   GridFile(String parent, String child, Cache<String, Metadata> metadataCache, int chunk_size, GridFilesystem fs) {
      this(parent + File.separator + child, metadataCache, chunk_size, fs);
   }

   GridFile(File parent, String child, Cache<String, Metadata> metadataCache, int chunk_size, GridFilesystem fs) {
      this(parent.getPath(), child, metadataCache, chunk_size, fs);
   }

   @Override
   public String getName() {
      return name;
   }

   /**
    * Returns path of this file. To avoid issues arising from file separator differences between different
    * operative systems, the path returned always uses Unix-like path separator, '/' character. Any client
    * code calling this method should bear that if disecting the path.
    *
    * @return String containing path of file.
    */
   @Override
   public String getPath() {
      return formatPath(super.getPath());

   }

   private String formatPath(String path) {
      if (path == null)
         return null;

      // Regardless of platform, always use the same separator char, otherwise
      // keys might not be found when transfering metadata between different OS
      path = path.replace('\\', SEPARATOR_CHAR);
      if (path != null && path.endsWith(SEPARATOR)) {
         int index = path.lastIndexOf(SEPARATOR);
         if (index != -1)
            path = path.substring(0, index);
      }
      return path;
   }

   @Override
   public long length() {
      Metadata metadata = getMetadata();
      if (metadata != null)
         return metadata.length;
      return 0;
   }

   private Metadata getMetadata() {
      if (isRootDir()) {
         return ROOT_DIR_METADATA;
      }
      return metadataCache.get(getPath());
   }

   private boolean isRootDir() {
      return "".equals(getPath());
   }

   void setLength(int new_length) {
      Metadata metadata = getMetadata();
      if (metadata == null)
         throw new IllegalStateException("metadata for " + getPath() + " not found.");

      metadata.setLength(new_length);
      metadata.setModificationTime(System.currentTimeMillis());
      metadataCache.put(getPath(), metadata);
   }

   public int getChunkSize() {
      return chunk_size;
   }

   @Override
   public boolean createNewFile() throws IOException {
      if (exists())
         return false;
      if (!checkParentDirs(getPath(), false))
         throw new IOException("Cannot create file " + getPath() + " (parent dir does not exist)");
      metadataCache.withFlags(FORCE_SYNCHRONOUS).put(getPath(), new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.FILE));
      return true;
   }

   @Override
   public boolean delete() {
      return delete(false); // asynchronous delete by default
   }

   public boolean delete(boolean synchronous) {
      if (!exists())
         return false;

      if (isDirectory() && hasChildren())
         return false;

      fs.remove(getPath(), synchronous);    // removes all the chunks belonging to the file
      if (synchronous)
         metadataCache.withFlags(FORCE_SYNCHRONOUS).remove(getPath()); // removes the metadata information
      else
         metadataCache.remove(getPath()); // removes the metadata information
      return true;
   }

   private boolean hasChildren() {
      File[] files = listFiles();
      return files != null && files.length > 0;
   }

   @Override
   public boolean mkdir() {
      return mkdir(false);
   }

   @Override
   public boolean mkdirs() {
      return mkdir(true);
   }

   private boolean mkdir(boolean alsoCreateParentDirs) {
      try {
         boolean parentsExist = checkParentDirs(getPath(), alsoCreateParentDirs);
         if (!parentsExist)
            return false;
         metadataCache.withFlags(FORCE_SYNCHRONOUS).put(getPath(),new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR));
         return true;
      }
      catch (IOException e) {
         return false;
      }
   }

   @Override
   public boolean exists() {
      return getMetadata() != null;
   }

   @Override
   public String getParent() {
      return formatPath(super.getParent());
   }

   @Override
   public File getParentFile() {
      String parentPath = getParent();
      if (parentPath == null)
         return null;
      return new GridFile(parentPath, metadataCache, chunk_size, fs);
   }

   @Override
   public long lastModified() {
      Metadata metadata = getMetadata();
      return metadata == null ? 0 : metadata.getModificationTime();
   }

   @Override
   public String[] list() {
      return list(null);
   }

   @Override
   public String[] list(FilenameFilter filter) {
      return _list(filter);
   }

   @Override
   public File[] listFiles() {
      return listFiles((FilenameFilter) null);
   }

   @Override
   public File[] listFiles(FilenameFilter filter) {
      return _listFiles(filter);
   }

   @Override
   public File[] listFiles(FileFilter filter) {
      return _listFiles(filter);
   }

   @Override
   public boolean isDirectory() {
      Metadata metadata = getMetadata();
      return metadata != null && metadata.isDirectory();
   }

   @Override
   public boolean isFile() {
      Metadata metadata = getMetadata();
      return metadata != null && metadata.isFile();
   }

   protected void initChunkSizeFromMetadata() {
      Metadata metadata = getMetadata();
      if (metadata != null)
         this.chunk_size = metadata.getChunkSize();
   }

   protected File[] _listFiles(Object filter) {
      String[] files = _list(filter);
      if (files == null)
         return null;
      File[] retval = new File[files.length];
      for (int i = 0; i < files.length; i++)
         retval[i] = new GridFile(this, files[i], metadataCache, chunk_size, fs);
      return retval;
   }


   protected String[] _list(Object filter) {
      if (!isDirectory())
         return null;

      Set<String> keys = metadataCache.keySet();
      Collection<String> list = new LinkedList<String>();
      for (String str : keys) {
         if (isChildOf(getPath(), str)) {
            if (filter instanceof FilenameFilter && !((FilenameFilter) filter).accept(new File(name), filename(str)))
               continue;
            else if (filter instanceof FileFilter && !((FileFilter) filter).accept(new File(str)))
               continue;
            list.add(filename(str));
         }
      }
      return list.toArray(new String[list.size()]);
   }

   /**
    * Verifies whether child is a child (dir or file) of parent
    *
    * @param parent
    * @param child
    * @return True if child is a child, false otherwise
    */
   protected static boolean isChildOf(String parent, String child) {
      if (parent == null || child == null)
         return false;
      if (!child.startsWith(parent))
         return false;
      if (child.length() <= parent.length())
         return false;
      int from = parent.equals(SEPARATOR) ? parent.length() : parent.length() + 1;
      //  if(from-1 > child.length())
      // return false;
      String[] comps = Util.components(child.substring(from), SEPARATOR);
      return comps != null && comps.length <= 1;
   }

   protected static String filename(String full_path) {
      String[] comps = Util.components(full_path, SEPARATOR);
      return comps != null ? comps[comps.length - 1] : null;
   }


   /**
    * Checks whether the parent directories are present (and are directories). If create_if_absent is true,
    * creates missing dirs
    *
    * @param path
    * @param create_if_absent
    * @return
    */
   protected boolean checkParentDirs(String path, boolean create_if_absent) throws IOException {
      String[] components = Util.components(path, SEPARATOR);
      if (components == null)
         return false;
      if (components.length == 1) // no parent directories to create, e.g. "data.txt"
         return true;

      StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (int i = 0; i < components.length - 1; i++) {
         String tmp = components[i];
         if (!tmp.equals(SEPARATOR)) {
            if (first)
               first = false;
            else
               sb.append(SEPARATOR);
         }
         sb.append(tmp);
         String comp = sb.toString();
         if (comp.equals(SEPARATOR))
            continue;
         Metadata val = exists(comp);
         if (val != null) {
            if (val.isFile())
               throw new IOException(format("cannot create %s as component %s is a file", path, comp));
         } else if (create_if_absent) {
            metadataCache.put(comp, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR));
         } else {
            // Couldn't find a component and we're not allowed to create components!
            return false;
         }

      }
      // check that we have found all the components we need.
      return true;
   }


   protected static String trim(String str) {
      if (str == null) return null;
      str = str.trim();
      if (str.equals(File.separator))
         return str;
      String[] comps = Util.components(str, File.separator);
      return comps != null && comps.length > 0 ? comps[comps.length - 1] : null;
   }

   private Metadata exists(String key) {
      return metadataCache.get(key);
   }

   public static class Metadata implements Externalizable {
      public static final byte FILE = 1 << 0;
      public static final byte DIR = 1 << 1;

      private int length = 0;
      private long modification_time = 0;
      private int chunk_size = 0;
      private byte flags = 0;


      public Metadata() {
      }

      public Metadata(int length, long modification_time, int chunk_size, byte flags) {
         this.length = length;
         this.modification_time = modification_time;
         this.chunk_size = chunk_size;
         this.flags = flags;
      }

      public int getLength() {
         return length;
      }

      public void setLength(int length) {
         this.length = length;
      }

      public long getModificationTime() {
         return modification_time;
      }

      public void setModificationTime(long modification_time) {
         this.modification_time = modification_time;
      }

      public int getChunkSize() {
         return chunk_size;
      }

      public boolean isFile() {
         return Util.isFlagSet(flags, FILE);
      }

      public boolean isDirectory() {
         return Util.isFlagSet(flags, DIR);
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append(getType());
         if (isFile())
            sb.append(", len=" + Util.printBytes(length) + ", chunk_size=" + chunk_size);
         sb.append(", mod_time=" + new Date(modification_time));
         return sb.toString();
      }

      private String getType() {
         if (isFile())
            return "file";
         if (isDirectory())
            return "dir";
         return "n/a";
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(length);
         out.writeLong(modification_time);
         out.writeInt(chunk_size);
         out.writeByte(flags);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         length = in.readInt();
         modification_time = in.readLong();
         chunk_size = in.readInt();
         flags = in.readByte();
      }
   }
}
