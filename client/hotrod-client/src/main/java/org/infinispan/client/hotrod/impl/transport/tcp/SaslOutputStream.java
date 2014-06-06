package org.infinispan.client.hotrod.impl.transport.tcp;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * SaslOutputStream.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslOutputStream extends OutputStream {

   private static final int BUFFER_SIZE = 64 * 1024;
   private final OutputStream outStream;
   private final SaslClient saslClient;
   private final int bufferSize;
   private final ByteArrayOutputStream buffer;

   public SaslOutputStream(OutputStream outStream, SaslClient saslClient) {
      this.saslClient = saslClient;
      this.outStream = new BufferedOutputStream(outStream, BUFFER_SIZE);
      String maxSendBuf = (String) saslClient.getNegotiatedProperty(Sasl.RAW_SEND_SIZE);
      if (maxSendBuf != null) {
          bufferSize = Integer.parseInt(maxSendBuf);
      } else {
          bufferSize = BUFFER_SIZE;
      }
      buffer = new ByteArrayOutputStream(bufferSize);
   }

   @Override
   public void write(int b) throws IOException {
      checkCapacity(1);
      buffer.write(b);
   }

   @Override
   public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      checkCapacity(len);
      buffer.write(b, off, len);
   }

   private void writeInt(int i) throws IOException {
      outStream.write((i >>> 24) & 0xFF);
      outStream.write((i >>> 16) & 0xFF);
      outStream.write((i >>>  8) & 0xFF);
      outStream.write((i >>>  0) & 0xFF);
   }

   private void checkCapacity(int capacity) throws IOException {
      if (buffer.size() + capacity >= bufferSize) {
         flush();
      }
   }

   private void wrapAndWrite() throws IOException {
      try {
         byte[] saslToken = saslClient.wrap(buffer.toByteArray(), 0, buffer.size());
         writeInt(saslToken.length);
         outStream.write(saslToken);
         buffer.reset();
      } catch (SaslException se) {
         try {
            saslClient.dispose();
         } catch (SaslException ignored) {
         }
         throw se;
      }
   }
   /**
    * Flushes this output stream
    *
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public void flush() throws IOException {
      wrapAndWrite();
      outStream.flush();
   }

   /**
    * Closes this output stream and releases any system resources associated with this stream.
    *
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public void close() throws IOException {
      saslClient.dispose();
      outStream.close();
   }
}
