/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.util.stream;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A collection of stream related utility methods.
 * <p/>
 * <p>Exceptions that are thrown and not explicitly declared are ignored.
 *
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @since 4.2
 */
public class Streams {

   private static final Log log = LogFactory.getLog(Streams.class);

   /////////////////////////////////////////////////////////////////////////
   //                               Closing                               //
   /////////////////////////////////////////////////////////////////////////

   /**
    * Attempt to close an <tt>InputStream</tt>.
    *
    * @param stream <tt>InputStream</tt> to attempt to close.
    * @return <tt>True</tt> if stream was closed (or stream was null), or
    *         <tt>false</tt> if an exception was thrown.
    */
   public static boolean close(final InputStream stream) {
      // do not attempt to close null stream, but return sucess
      if (stream == null) {
         return true;
      }

      boolean success = true;

      try {
         stream.close();
      } catch (IOException e) {
         success = false;
      }

      return success;
   }

   /**
    * Attempt to close an <tt>OutputStream</tt>.
    *
    * @param stream <tt>OutputStream</tt> to attempt to close.
    * @return <tt>True</tt> if stream was closed (or stream was null), or
    *         <tt>false</tt> if an exception was thrown.
    */
   public static boolean close(final OutputStream stream) {
      // do not attempt to close null stream, but return sucess
      if (stream == null) {
         return true;
      }

      boolean success = true;

      try {
         stream.close();
      } catch (IOException e) {
         success = false;
      }

      return success;
   }

   /**
    * Attempt to close an <tt>InputStream</tt> or <tt>OutputStream</tt>.
    *
    * @param stream Stream to attempt to close.
    * @return <tt>True</tt> if stream was closed (or stream was null), or
    *         <tt>false</tt> if an exception was thrown.
    * @throws IllegalArgumentException Stream is not an <tt>InputStream</tt> or
    *                                  <tt>OuputStream</tt>.
    */
   public static boolean close(final Object stream) {
      boolean success;

      if (stream instanceof InputStream) {
         success = close((InputStream) stream);
      } else if (stream instanceof OutputStream) {
         success = close((OutputStream) stream);
      } else {
         throw new IllegalArgumentException
               ("stream is not an InputStream or OutputStream");
      }

      return success;
   }

   /**
    * Attempt to close an array of <tt>InputStream</tt>s.
    *
    * @param streams Array of <tt>InputStream</tt>s to attempt to close.
    * @return <tt>True</tt> if all streams were closed, or <tt>false</tt> if an
    *         exception was thrown.
    */
   public static boolean close(final InputStream[] streams) {
      boolean success = true;

      for (InputStream stream : streams) {
         boolean rv = close(stream);
         if (!rv) success = false;
      }

      return success;
   }

   /**
    * Attempt to close an array of <tt>OutputStream</tt>s.
    *
    * @param streams Array of <tt>OutputStream</tt>s to attempt to close.
    * @return <tt>True</tt> if all streams were closed, or <tt>false</tt> if an
    *         exception was thrown.
    */
   public static boolean close(final OutputStream[] streams) {
      boolean success = true;

      for (OutputStream stream : streams) {
         boolean rv = close(stream);
         if (!rv) success = false;
      }

      return success;
   }

   /**
    * Attempt to close an array of <tt>InputStream</tt>a and/or
    * <tt>OutputStream</tt>s.
    *
    * @param streams Array of streams to attempt to close.
    * @return <tt>True</tt> if all streams were closed, or <tt>false</tt> if an
    *         exception was thrown.
    * @throws IllegalArgumentException Stream is not an <tt>InputStream</tt> or
    *                                  <tt>OuputStream</tt>.  Closing stops at
    *                                  the last valid stream object in this
    *                                  case.
    */
   public static boolean close(final Object[] streams) {
      boolean success = true;

      for (Object stream : streams) {
         boolean rv = close(stream);
         if (!rv) success = false;
      }

      return success;
   }

   /**
    * Attempt to flush and close an <tt>OutputStream</tt>.
    *
    * @param stream <tt>OutputStream</tt> to attempt to flush and close.
    * @return <tt>True</tt> if stream was flushed and closed, or <tt>false</tt>
    *         if an exception was thrown.
    */
   public static boolean fclose(final OutputStream stream) {
      return flush(stream) && close(stream);
   }

   /**
    * Attempt to flush and close an array of <tt>OutputStream</tt>s.
    *
    * @param streams <tt>OutputStream</tt>s to attempt to flush and close.
    * @return <tt>True</tt> if all streams were flushed and closed, or
    *         <tt>false</tt> if an exception was thrown.
    */
   public static boolean fclose(final OutputStream[] streams) {
      boolean success = true;

      for (OutputStream stream : streams) {
         boolean rv = fclose(stream);
         if (!rv) success = false;
      }

      return success;
   }


   /////////////////////////////////////////////////////////////////////////
   //                                Flushing                             //
   /////////////////////////////////////////////////////////////////////////

   /**
    * Attempt to flush an <tt>OutputStream</tt>.
    *
    * @param stream <tt>OutputStream</tt> to attempt to flush.
    * @return <tt>True</tt> if stream was flushed (or stream was null), or
    *         <tt>false</tt> if an exception was thrown.
    */
   public static boolean flush(final OutputStream stream) {
      // do not attempt to close null stream, but return sucess
      if (stream == null) {
         return true;
      }

      boolean success = true;

      try {
         stream.flush();
      } catch (IOException e) {
         success = false;
      }

      return success;
   }

   /**
    * Attempt to flush an array of <tt>OutputStream</tt>s.
    *
    * @param streams <tt>OutputStream</tt>s to attempt to flush.
    * @return <tt>True</tt> if all streams were flushed, or <tt>false</tt> if
    *         an exception was thrown.
    */
   public static boolean flush(final OutputStream[] streams) {
      boolean success = true;

      for (OutputStream stream : streams) {
         boolean rv = flush(stream);
         if (!rv) success = false;
      }

      return success;
   }


   /////////////////////////////////////////////////////////////////////////
   //                                  Misc                               //
   /////////////////////////////////////////////////////////////////////////

   /**
    * The default buffer size that will be used for buffered operations.
    */
   public static final int DEFAULT_BUFFER_SIZE = 2048;

   /**
    * Copy all of the bytes from the input stream to the output stream.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @param buffer The buffer to use while copying.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copy(final InputStream input,
                           final OutputStream output,
                           final byte buffer[])
         throws IOException {
      long total = 0;
      int read;

      boolean trace = log.isTraceEnabled();
      if (trace) {
         log.tracef("copying %s to %s with buffer size: %d", input, output, buffer.length);
      }

      while ((read = input.read(buffer)) != -1) {
         output.write(buffer, 0, read);
         total += read;

         if (trace) {
            log.tracef("bytes read: %d; total bytes read: %d", read, total);
         }
      }

      return total;
   }

   /**
    * Copy all of the bytes from the input stream to the output stream.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @param size   The size of the buffer to use while copying.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copy(final InputStream input,
                           final OutputStream output,
                           final int size)
         throws IOException {
      return copy(input, output, new byte[size]);
   }

   /**
    * Copy all of the bytes from the input stream to the output stream.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copy(final InputStream input,
                           final OutputStream output)
         throws IOException {
      return copy(input, output, DEFAULT_BUFFER_SIZE);
   }

   /**
    * Copy all of the bytes from the input stream to the output stream wrapping
    * streams in buffers as needed.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copyb(InputStream input,
                            OutputStream output)
         throws IOException {
      if (!(input instanceof BufferedInputStream)) {
         input = new BufferedInputStream(input);
      }

      if (!(output instanceof BufferedOutputStream)) {
         output = new BufferedOutputStream(output);
      }

      long bytes = copy(input, output, DEFAULT_BUFFER_SIZE);

      output.flush();

      return bytes;
   }

   /**
    * Copy a limited number of bytes from the input stream to the output
    * stream.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @param buffer The buffer to use while copying.
    * @param length The maximum number of bytes to copy.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copySome(final InputStream input,
                               final OutputStream output,
                               final byte buffer[],
                               final long length)
         throws IOException {
      long total = 0;
      int read;
      int readLength;

      boolean trace = log.isTraceEnabled();

      // setup the initial readLength, if length is less than the buffer
      // size, then we only want to read that much
      readLength = Math.min((int) length, buffer.length);
      if (trace) {
         log.tracef("initial read length: %d", readLength);
      }

      while (readLength != 0 && (read = input.read(buffer, 0, readLength)) != -1) {
         if (trace) log.tracef("read bytes: %d", read);
         output.write(buffer, 0, read);
         total += read;
         if (trace) log.tracef("total bytes read: %d", total);

         // update the readLength
         readLength = Math.min((int) (length - total), buffer.length);
         if (trace) log.tracef("next read length: %d", readLength);
      }

      return total;
   }

   /**
    * Copy a limited number of bytes from the input stream to the output
    * stream.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @param size   The size of the buffer to use while copying.
    * @param length The maximum number of bytes to copy.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copySome(final InputStream input,
                               final OutputStream output,
                               final int size,
                               final long length)
         throws IOException {
      return copySome(input, output, new byte[size], length);
   }

   /**
    * Copy a limited number of bytes from the input stream to the output
    * stream.
    *
    * @param input  Stream to read bytes from.
    * @param output Stream to write bytes to.
    * @param length The maximum number of bytes to copy.
    * @return The total number of bytes copied.
    * @throws IOException Failed to copy bytes.
    */
   public static long copySome(final InputStream input,
                               final OutputStream output,
                               final long length)
         throws IOException {
      return copySome(input, output, DEFAULT_BUFFER_SIZE, length);
   }
}
