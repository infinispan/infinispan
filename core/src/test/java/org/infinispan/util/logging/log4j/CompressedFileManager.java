/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.infinispan.util.logging.log4j;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.appender.FileManager;
import org.apache.logging.log4j.core.appender.ManagerFactory;

/**
 * Manages actual File I/O for the CompressedFileAppender.
 */
public class CompressedFileManager extends FileManager {

   protected CompressedFileManager(String fileName, OutputStream os, boolean append, boolean locking, String advertiseURI, Layout<? extends Serializable> layout, int bufferSize) {
      super(fileName, os, append, locking, advertiseURI, layout, bufferSize);
   }

   private static final CompressedFileManagerFactory FACTORY = new CompressedFileManagerFactory();


   /**
    * Returns the FileManager.
    *
    * @param fileName
    *           The name of the file to manage.
    * @param append
    *           true if the file should be appended to, false if it should be overwritten.
    * @param locking
    *           true if the file should be locked while writing, false otherwise.
    * @param bufferedIo
    *           true if the contents should be buffered as they are written.
    * @param advertiseUri
    *           the URI to use when advertising the file
    * @param layout
    *           The layout
    * @param bufferSize
    *           buffer size for buffered IO
    * @return A FileManager for the File.
    */
   public static CompressedFileManager getFileManager(final String fileName, final boolean append, boolean locking, final boolean bufferedIo, final String advertiseUri,
         final Layout<? extends Serializable> layout, final int bufferSize) {

      if (locking && bufferedIo) {
         locking = false;
      }
      return (CompressedFileManager) getManager(fileName, new FactoryData(append, locking, bufferedIo, bufferSize, advertiseUri, layout), FACTORY);
   }

   /**
    * Factory Data.
    */
   private static class FactoryData {
      private final boolean append;
      private final boolean locking;
      private final boolean bufferedIO;
      private final int bufferSize;
      private final String advertiseURI;
      private final Layout<? extends Serializable> layout;

      /**
       * Constructor.
       *
       * @param append
       *           Append status.
       * @param locking
       *           Locking status.
       * @param bufferedIO
       *           Buffering flag.
       * @param bufferSize
       *           Buffer size.
       * @param advertiseURI
       *           the URI to use when advertising the file
       */
      public FactoryData(final boolean append, final boolean locking, final boolean bufferedIO, final int bufferSize, final String advertiseURI,
            final Layout<? extends Serializable> layout) {
         this.append = append;
         this.locking = locking;
         this.bufferedIO = bufferedIO;
         this.bufferSize = bufferSize;
         this.advertiseURI = advertiseURI;
         this.layout = layout;
      }
   }

   /**
    * Factory to create a FileManager.
    */
   private static class CompressedFileManagerFactory implements ManagerFactory<CompressedFileManager, FactoryData> {

      /**
       * Create a FileManager.
       *
       * @param name
       *           The name of the File.
       * @param data
       *           The FactoryData
       * @return The FileManager for the File.
       */
      @Override
      public CompressedFileManager createManager(final String name, final FactoryData data) {
         final File file = new File(name);
         final File parent = file.getParentFile();
         if (null != parent && !parent.exists()) {
            parent.mkdirs();
         }

         OutputStream os;
         try {
            os = new FileOutputStream(name, data.append);
            int bufferSize = data.bufferSize;
            if (name.endsWith(".gz")) {
               os = new GZIPOutputStream(os, bufferSize);
               os.flush();
            } else {
               if (data.bufferedIO) {
                  os = new BufferedOutputStream(os, bufferSize);
               } else {
                  bufferSize = -1; // signals to RollingFileManager not to use BufferedOutputStream
               }
            }
            return new CompressedFileManager(name, os, data.append, data.locking, data.advertiseURI, data.layout, bufferSize);
         } catch (final IOException ex) {
            LOGGER.error("FileManager (" + name + ") " + ex);
         }
         return null;
      }
   }

}
