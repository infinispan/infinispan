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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

import static org.infinispan.context.Flag.FORCE_SYNCHRONOUS;

/**
 * Subclass of File to iterate through directories and files in a grid
 *
 * @author Bela Ban
 */
public class GridFile extends File {
   private static final long serialVersionUID = -6729548421029004260L;
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
      initMetadata();
   }

   GridFile(String parent, String child, Cache<String, Metadata> metadataCache, int chunk_size, GridFilesystem fs) {
      super(parent, child);
      this.fs = fs;
      this.name = trim(parent + File.separator + child);
      this.metadataCache = metadataCache.getAdvancedCache();
      this.chunk_size = chunk_size;
      initMetadata();
   }

   GridFile(File parent, String child, Cache<String, Metadata> metadataCache, int chunk_size, GridFilesystem fs) {
      super(parent, child);
      this.fs = fs;
      this.name = trim(parent.getAbsolutePath() + File.separator + child);
      this.metadataCache = metadataCache.getAdvancedCache();
      this.chunk_size = chunk_size;
      initMetadata();
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
      String my_path = super.getPath();
      // Regardless of platform, always use the same separator char, otherwise
      // keys might not be found when transfering metadata between different OS
      my_path = my_path.replace('\\', SEPARATOR_CHAR);
      if (my_path != null && my_path.endsWith(SEPARATOR)) {
         int index = my_path.lastIndexOf(SEPARATOR);
         if (index != -1)
            my_path = my_path.substring(0, index);
      }
      return my_path;
   }

   @Override
   public long length() {
      Metadata metadata = metadataCache.get(getPath());
      if (metadata != null)
         return metadata.length;
      return 0;
   }

   void setLength(int new_length) {
      Metadata metadata = metadataCache.get(getPath());
      if (metadata != null) {
         metadata.length = new_length;
         metadata.setModificationTime(System.currentTimeMillis());
         metadataCache.put(getPath(), metadata);
      } else
         System.err.println("metadata for " + getPath() + " not found !");
   }

   public int getChunkSize() {
      return chunk_size;
   }

   @Override
   public boolean createNewFile() throws IOException {
      if (exists())
         return true;
      if (!checkParentDirs(getPath(), false))
         return false;
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
      if (isFile()) {
         fs.remove(getPath(), synchronous);    // removes all the chunks belonging to the file
         if (synchronous)
            metadataCache.withFlags(FORCE_SYNCHRONOUS).remove(getPath()); // removes the metadata information
         else
            metadataCache.remove(getPath()); // removes the metadata information
         return true;
      }
      if (isDirectory()) {
         File[] files = listFiles();
         if (files != null && files.length > 0)
            return false;
         fs.remove(getPath(), synchronous);    // removes all the chunks belonging to the file
         if (synchronous)
            metadataCache.withFlags(FORCE_SYNCHRONOUS).remove(getPath()); // removes the metadata information
         else
            metadataCache.remove(getPath()); // removes the metadata information
      }
      return true;
   }

   @Override
   public boolean mkdir() {
      try {
         boolean parents_exist = checkParentDirs(getPath(), false);
         if (!parents_exist)
            return false;
         metadataCache.withFlags(FORCE_SYNCHRONOUS).put(getPath(), new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR));
         return true;
      }
      catch (IOException e) {
         e.printStackTrace();
         return false;
      }
   }

   @Override
   public boolean mkdirs() {
      try {
         boolean parents_exist = checkParentDirs(getPath(), true);
         if (!parents_exist)
            return false;
         metadataCache.withFlags(FORCE_SYNCHRONOUS).put(getPath(), new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR));
         return true;
      }
      catch (IOException e) {
         return false;
      }
   }

   @Override
   public boolean exists() {
      return metadataCache.get(getPath()) != null;
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
      Metadata val = metadataCache.get(getPath());
      return val == null || val.isDirectory();
   }

   @Override
   public boolean isFile() {
      Metadata val = metadataCache.get(getPath());
      return val == null || val.isFile();
   }

   protected void initMetadata() {
      Metadata metadata = metadataCache.get(getPath());
      if (metadata != null)
         this.chunk_size = metadata.getChunkSize();
   }

   protected File[] _listFiles(Object filter) {
      String[] files = _list(filter);
      if (files == null)
         return new File[0];
      File[] retval = new File[files.length];
      for (int i = 0; i < files.length; i++)
         retval[i] = new GridFile(files[i], metadataCache, chunk_size, fs);
      return retval;
   }


   protected String[] _list(Object filter) {
      Set<String> keys = metadataCache.keySet();
      if (keys == null)
         return null;
      Collection<String> list = new ArrayList<String>(keys.size());
      for (String str : keys) {
         if (isChildOf(getPath(), str)) {
            if (filter instanceof FilenameFilter && !((FilenameFilter) filter).accept(new File(name), filename(str)))
               continue;
            else if (filter instanceof FileFilter && !((FileFilter) filter).accept(new File(str)))
               continue;
            list.add(str);
         }
      }
      String[] retval = new String[list.size()];
      int index = 0;
      for (String tmp : list)
         retval[index++] = tmp;
      return retval;
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
      int from = parent.equals(File.separator) ? parent.length() : parent.length() + 1;
      //  if(from-1 > child.length())
      // return false;
      String[] comps = Util.components(child.substring(from), File.separator);
      return comps != null && comps.length <= 1;
   }

   protected static String filename(String full_path) {
      String[] comps = Util.components(full_path, File.separator);
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
         Metadata val = exists(comp);
         if (val != null && val.isFile())
            throw new IOException(String.format(
                  "cannot create %s as component %s is a file", path, comp));
         else if (create_if_absent)
            metadataCache.put(comp, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR));
         else
            return false;
      }
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
         boolean is_file = Util.isFlagSet(flags, FILE);
         StringBuilder sb = new StringBuilder();
         sb.append(getType());
         if (is_file)
            sb.append(", len=" + Util.printBytes(length) + ", chunk_size=" + chunk_size);
         sb.append(", mod_time=" + new Date(modification_time));
         return sb.toString();
      }

      private String getType() {
         if (Util.isFlagSet(flags, FILE))
            return "file";
         if (Util.isFlagSet(flags, DIR))
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
