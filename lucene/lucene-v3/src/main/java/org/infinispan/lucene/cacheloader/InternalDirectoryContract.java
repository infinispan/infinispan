package org.infinispan.lucene.cacheloader;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * This is not meant as a public API but as an internal contract
 * to make it possible to use different versions of Lucene
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
public interface InternalDirectoryContract {

   String[] listAll() throws IOException;

   long fileLength(String fileName) throws IOException;

   void close() throws IOException;

   long fileModified(String fileName) throws IOException;

   IndexInput openInput(String fileName) throws IOException;

   boolean fileExists(String fileName) throws IOException;

}
