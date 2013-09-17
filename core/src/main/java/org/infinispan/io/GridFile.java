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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Set;

import static java.lang.String.format;

/**
 * Subclass of File to iterate through directories and files in a grid
 *
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridFile extends File {
   private static final long serialVersionUID = 552534285862004134L;
   private static final Metadata ROOT_DIR_METADATA = new Metadata(0, 0, 0, Metadata.DIR);
   private static final char SEPARATOR_CHAR = '/';
   private static final String SEPARATOR = "" + SEPARATOR_CHAR;
   private final AdvancedCache<String, Metadata> metadataCache;
   private final GridFilesystem fs;
   private final String path;
   private int chunkSize;

   /**
    * Creates a GridFile instance
    * @param pathname path of file
    * @param metadataCache cache to use to store metadata
    * @param chunkSize chunk size.  Will be upgraded to next highest power of two.
    * @param fs GridFilesystem instance
    */
   GridFile(String pathname, Cache<String, Metadata> metadataCache, int chunkSize, GridFilesystem fs) {
      super(pathname);
      this.fs = fs;
      this.path = formatPath(pathname);
      this.metadataCache = metadataCache.getAdvancedCache();
      this.chunkSize = ModularArithmetic.CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO ? chunkSize : org.infinispan.commons.util.Util.findNextHighestPowerOfTwo(chunkSize);
      initChunkSizeFromMetadata();
   }

   GridFile(String parent, String child, Cache<String, Metadata> metadataCache, int chunkSize, GridFilesystem fs) {
      this(parent + File.separator + child, metadataCache, chunkSize, fs);
   }

   GridFile(File parent, String child, Cache<String, Metadata> metadataCache, int chunkSize, GridFilesystem fs) {
      this(parent.getPath(), child, metadataCache, chunkSize, fs);
   }

   @Override
   public String getName() {
      return filename(getPath());
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
      return path;
   }

   @Override
   public String getAbsolutePath() {
      return convertToAbsolute(getPath());
   }

   @Override
   public File getAbsoluteFile() {
      return new GridFile(getAbsolutePath(), metadataCache, getChunkSize(), fs);
   }

   @Override
   public String getCanonicalPath() throws IOException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public File getCanonicalFile() throws IOException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean isAbsolute() {
      return getPath().startsWith(SEPARATOR);
   }

   @Override
   public boolean renameTo(File dest) {
      // implementing this based on the current storage structure is complex and very expensive.
      // a redesign is nessesary, especially must be avoid storing paths as key
      // maybe file name + reference to the parent in metadata and as key is used a uuid, so movements or renames
      // are only affected on the current file. metadata should also contain list of uuids of the corresponding data chunks
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void deleteOnExit() {
      // there exists no pre-CacheShutdown event, so unable to remove the entry
      throw new UnsupportedOperationException("Not implemented");
   }

   private String convertToAbsolute(String path) {
      if (!path.startsWith(SEPARATOR))
         return SEPARATOR + path;
      else
         return path;
   }

   private static String formatPath(String path) {
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
      return metadataCache.get(getAbsolutePath());
   }

   private boolean isRootDir() {
      return "/".equals(getAbsolutePath());
   }

   void setLength(int newLength) {
      Metadata metadata = getMetadata();
      if (metadata == null)
         throw new IllegalStateException("metadata for " + getAbsolutePath() + " not found.");

      metadata.setLength(newLength);
      metadata.setModificationTime(System.currentTimeMillis());
      metadataCache.put(getAbsolutePath(), metadata);
   }

   /**
    * Guaranteed to be a power of two
    */
   public int getChunkSize() {
      return chunkSize;
   }

   @Override
   public boolean createNewFile() throws IOException {
      if (exists())
         return false;
      if (!checkParentDirs(getAbsolutePath(), false))
         throw new IOException("Cannot create file " + getAbsolutePath() + " (parent dir does not exist)");
      metadataCache.put(getAbsolutePath(), new Metadata(0, System.currentTimeMillis(), chunkSize, Metadata.FILE));
      return true;
   }

   @Override
   public boolean delete() {
      if (!exists())
         return false;

      if (isDirectory() && hasChildren())
         return false;

      fs.remove(getAbsolutePath());    // removes all the chunks belonging to the file
      metadataCache.remove(getAbsolutePath()); // removes the metadata information
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
         boolean parentsExist = checkParentDirs(getAbsolutePath(), alsoCreateParentDirs);
         if (!parentsExist)
            return false;
         metadataCache.put(getAbsolutePath(),new Metadata(0, System.currentTimeMillis(), chunkSize, Metadata.DIR));
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
      return new GridFile(parentPath, metadataCache, chunkSize, fs);
   }

   @Override
   public long lastModified() {
      Metadata metadata = getMetadata();
      return metadata == null ? 0 : metadata.getModificationTime();
   }

   @Override
   public boolean setLastModified(long time) {
      if (time < 0){
         throw new IllegalArgumentException("Negative time");
      }
      Metadata metadata = getMetadata();
      if(metadata == null){
         return false;
      }
      metadata.setModificationTime(time);
      metadataCache.put(getAbsolutePath(), metadata);
      return true;
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
         this.chunkSize = metadata.getChunkSize();
   }

   protected File[] _listFiles(Object filter) {
      String[] filenames = _list(filter);
      return convertFilenamesToFiles(filenames);
   }

   private File[] convertFilenamesToFiles(String[] files) {
      if (files == null)
         return null;
      File[] retval = new File[files.length];
      for (int i = 0; i < files.length; i++)
         retval[i] = new GridFile(this, files[i], metadataCache, chunkSize, fs);
      return retval;
   }


   protected String[] _list(Object filter) {
      if (!isDirectory())
         return null;

      Set<String> paths = metadataCache.keySet();
      Collection<String> list = new LinkedList<String>();
      for (String path : paths) {
         if (isChildOf(getAbsolutePath(), path)) {
            if (filter instanceof FilenameFilter && !((FilenameFilter) filter).accept(this, filename(path)))
               continue;
            else if (filter instanceof FileFilter && !((FileFilter) filter).accept(new File(path)))
               continue;
            list.add(filename(path));
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
      if (!child.startsWith((parent.endsWith(SEPARATOR) ? parent : parent + SEPARATOR)))
         return false;
      if (child.length() <= parent.length())
         return false;
      int from = parent.equals(SEPARATOR) ? parent.length() : parent.length() + 1;
      //  if(from-1 > child.length())
      // return false;
      String[] comps = Util.components(child.substring(from), SEPARATOR);
      return comps != null && comps.length <= 1;
   }

   protected static String filename(String fullPath) {
      String[] comps = Util.components(fullPath, SEPARATOR);
      return comps != null ? comps[comps.length - 1] : "";
   }


   /**
    * Checks whether the parent directories are present (and are directories). If createIfAbsent is true,
    * creates missing dirs
    *
    * @param path
    * @param createIfAbsent
    * @return
    */
   protected boolean checkParentDirs(String path, boolean createIfAbsent) throws IOException {
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
         } else if (createIfAbsent) {
            metadataCache.put(comp, new Metadata(0, System.currentTimeMillis(), chunkSize, Metadata.DIR));
         } else {
            // Couldn't find a component and we're not allowed to create components!
            return false;
         }

      }
      // check that we have found all the components we need.
      return true;
   }

   @Override
   public boolean equals(Object obj) {
      if ((obj != null) && (obj instanceof GridFile)) {
          return compareTo((GridFile)obj) == 0;
      }
      return false;
   }

   @Override
   public boolean canRead() {
      return isFile();
   }

   @Override
   public boolean canWrite() {
      return isFile();
   }

   @Override
   public boolean isHidden() {
      return false;
   }

   @Override
   public boolean canExecute() {
      return false;
   }

   @Override
   public int compareTo(File file) {
      return getAbsolutePath().compareTo(file.getAbsolutePath());
   }

   @Override
   public int hashCode() {
      return getAbsolutePath().hashCode();
   }

   @Override
   public String toString() {
      return "GridFile{" +
            "path='" + path + '\'' +
            '}';
   }

   @Override
   public URL toURL() throws MalformedURLException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public URI toURI() {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setReadOnly() {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setWritable(boolean writable, boolean ownerOnly) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setWritable(boolean writable) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setReadable(boolean readable, boolean ownerOnly) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setReadable(boolean readable) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setExecutable(boolean executable, boolean ownerOnly) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean setExecutable(boolean executable) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public long getTotalSpace() {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public long getFreeSpace() {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public long getUsableSpace() {
      throw new UnsupportedOperationException("Not implemented");
   }

   private Metadata exists(String key) {
      return metadataCache.get(key);
   }

   public static class Metadata implements Externalizable {
      public static final byte FILE = 1;
      public static final byte DIR = 1 << 1;

      private int length = 0;
      private long modificationTime = 0;
      private int chunkSize;
      private byte flags;


      public Metadata() {
         chunkSize = 1;
         flags = 0;
      }

      public Metadata(int length, long modificationTime, int chunkSize, byte flags) {
         this.length = length;
         this.modificationTime = modificationTime;
         this.chunkSize = ModularArithmetic.CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO ? chunkSize : org.infinispan.commons.util.Util.findNextHighestPowerOfTwo(chunkSize);
         this.flags = flags;
      }

      public int getLength() {
         return length;
      }

      public void setLength(int length) {
         this.length = length;
      }

      public long getModificationTime() {
         return modificationTime;
      }

      public void setModificationTime(long modificationTime) {
         this.modificationTime = modificationTime;
      }

      public int getChunkSize() {
         return chunkSize;
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
            sb.append(", len=" + Util.printBytes(length) + ", chunkSize=" + chunkSize);
         sb.append(", modTime=").append(new Date(modificationTime));
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
         out.writeLong(modificationTime);
         out.writeInt(chunkSize);
         out.writeByte(flags);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         length = in.readInt();
         modificationTime = in.readLong();
         chunkSize = in.readInt();
         flags = in.readByte();
      }
   }
}
