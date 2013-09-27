package org.infinispan.io;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Entry point for GridFile and GridInputStream / GridOutputStream
 *
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridFilesystem {

   private static final Log log = LogFactory.getLog(GridFilesystem.class);

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
      if(metadata.getCacheConfiguration().clustering().cacheMode().isClustered() &&
            !metadata.getCacheConfiguration().clustering().cacheMode().isSynchronous()){
         log.warnGridFSMetadataCacheRequiresSync();
      }
      this.data = data;
      this.metadata = metadata;
      this.defaultChunkSize = ModularArithmetic.CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO ? defaultChunkSize : Util.findNextHighestPowerOfTwo(defaultChunkSize);
   }

   public GridFilesystem(Cache<String, byte[]> data, Cache<String, GridFile.Metadata> metadata) {
      this(data, metadata, ModularArithmetic.CANNOT_ASSUME_DENOM_IS_POWER_OF_TWO ? 8000 : 8192);
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
      checkFileIsReadable(file);
      return new GridInputStream(file, data);
   }

   private void checkFileIsReadable(GridFile file) throws FileNotFoundException {
      checkFileExists(file);
      checkIsNotDirectory(file);
   }

   private void checkFileExists(GridFile file) throws FileNotFoundException {
      if (!file.exists())
         throw new FileNotFoundException(file.getPath());
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
    * Opens a ReadableGridFileChannel for reading from the file denoted by the given file path. One of the benefits
    * of using a channel over an InputStream is the possibility to randomly seek to any position in the file (see
    * #ReadableGridChannel.position()).
    * @param pathname path of the file to open for reading
    * @return a ReadableGridFileChannel for reading from the file
    * @throws FileNotFoundException if the file does not exist or is a directory
    */
   public ReadableGridFileChannel getReadableChannel(String pathname) throws FileNotFoundException {
      GridFile file = (GridFile) getFile(pathname);
      checkFileIsReadable(file);
      return new ReadableGridFileChannel(file, data);
   }

   /**
    * Opens a WritableGridFileChannel for writing to the file denoted by pathname. If a file at pathname already exists,
    * writing to the returned channel will overwrite the contents of the file.
    * @param pathname the path to write to
    * @return a WritableGridFileChannel for writing to the file at pathname
    * @throws IOException if an error occurs
    */
   public WritableGridFileChannel getWritableChannel(String pathname) throws IOException {
      return getWritableChannel(pathname, false);
   }

   /**
    * Opens a WritableGridFileChannel for writing to the file denoted by pathname. The channel can either overwrite the
    * existing file or append to it.
    * @param pathname the path to write to
    * @param append if true, the bytes written to the WritableGridFileChannel will be appended to the end of the file.
    *               If false, the bytes will overwrite the original contents.
    * @return a WritableGridFileChannel for writing to the file at pathname
    * @throws IOException if an error occurs
    */
   public WritableGridFileChannel getWritableChannel(String pathname, boolean append) throws IOException {
      return getWritableChannel(pathname, append, defaultChunkSize);
   }

   /**
    * Opens a WritableGridFileChannel for writing to the file denoted by pathname.
    * @param pathname the file to write to
    * @param append if true, the bytes written to the channel will be appended to the end of the file
    * @param chunkSize the size of the file's chunks. This parameter is honored only when the file at pathname does
    *        not yet exist. If the file already exists, the file's own chunkSize has precedence.
    * @return a WritableGridFileChannel for writing to the file
    * @throws IOException if the file is a directory, cannot be created or some other error occurs
    */
   public WritableGridFileChannel getWritableChannel(String pathname, boolean append, int chunkSize) throws IOException {
      GridFile file = (GridFile) getFile(pathname, chunkSize);
      checkIsNotDirectory(file);
      createIfNeeded(file);
      return new WritableGridFileChannel(file, data, append);
   }

   /**
    * Removes the file denoted by absolutePath.
    * @param absolutePath the absolute path of the file to remove
    */
   void remove(String absolutePath) {
      if (absolutePath == null)
         return;
      GridFile.Metadata md = metadata.get(absolutePath);
      if (md == null)
         return;

      int numChunks = md.getLength() / md.getChunkSize() + 1;
      for (int i = 0; i < numChunks; i++)
         data.remove(FileChunkMapper.getChunkKey(absolutePath, i));
   }
}
