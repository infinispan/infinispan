package org.infinispan.util.logging.log4j;

import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

/**
 * Appender that writes to a file and compresses the output using gzip.
 *
 * Based on <code>org.apache.log4j.FileAppender</code>
 */
public class CompressedFileAppender extends FileAppender {

   GZIPOutputStream gzos;

   static {
      final Thread shutdownThread = new Thread(new Runnable() {
         public void run() {
            // This will close all the appenders gracefully.
            LogManager.shutdown();
         }
      }, "Log4j shutdown thread");
      Runtime.getRuntime().addShutdownHook(shutdownThread);
   }

   @Override
   protected OutputStreamWriter createWriter(OutputStream os) {
      try {
         gzos = new GZIPOutputStream(os, bufferSize);
         gzos.flush();
         return super.createWriter(gzos);
      } catch (IOException e) {
         throw new RuntimeException("Unable to create gzipped output stream");
      }
   }

   @Override
   protected void reset() {
      if (gzos != null) {
         closeCompressor();
         gzos = null;
      }
      super.reset();
   }

   private void closeCompressor() {
      try {
         gzos.close();
      } catch (IOException e) {
         throw new RuntimeException("Unable to finish gzipped output stream");
      }
   }
}
