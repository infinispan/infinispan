package org.infinispan.lucene.cacheloader;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

/**
 * @author Sanne Grinovero
 * @since 5.2
 */
public class DirectoryV3Adaptor implements InternalDirectoryContract {

   private final Directory directory;

   public DirectoryV3Adaptor(Directory directory) {
      this.directory = directory;
   }

   @Override
   public String[] listAll() throws IOException {
      return directory.listAll();
   }

   @Override
   public long fileLength(final String fileName) throws IOException {
      return directory.fileLength(fileName);
   }

   @Override
   public void close() throws IOException {
      directory.close();
   }

   @Override
   public long fileModified(final String fileName) throws IOException {
      return directory.fileModified(fileName);
   }

   @Override
   public IndexInput openInput(final String fileName) throws IOException {
      return directory.openInput(fileName);
   }

   @Override
   public boolean fileExists(final String fileName) throws IOException {
      return directory.fileExists(fileName);
   }

}
