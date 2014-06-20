package org.infinispan.client.hotrod.impl.transport.tcp;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * SaslInputStream.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslInputStream extends InputStream {
   public static final Log LOG = LogFactory.getLog(SaslInputStream.class);

   private final SaslClient saslClient;
   private final DataInputStream inStream;
   private byte[] buffer;
   private int bufferPtr = 0;
   private int bufferLength = 0;


   public SaslInputStream(InputStream inStream, SaslClient saslClient) {
      this.inStream = new DataInputStream(new BufferedInputStream(inStream));
      this.saslClient = saslClient;
   }

   @Override
   public int read() throws IOException {
      if (bufferPtr >= bufferLength) {
         int i = 0;
         while (i == 0)
            i = fillBuffer();
         if (i == -1)
            return -1;
      }
      return ((int) buffer[bufferPtr++] & 0xff);
   }

   @Override
   public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      if (bufferPtr >= bufferLength) {
         int i = 0;
         while (i == 0)
            i = fillBuffer();
         if (i == -1)
            return -1;
      }
      if (len <= 0) {
         return 0;
      }
      int available = bufferLength - bufferPtr;
      if (len < available)
         available = len;
      if (b != null) {
         System.arraycopy(buffer, bufferPtr, b, off, available);
      }
      bufferPtr = bufferPtr + available;
      return available;
   }

   @Override
   public long skip(long n) throws IOException {
      int available = bufferLength - bufferPtr;
      if (n > available) {
         n = available;
      }
      if (n < 0) {
         return 0;
      }
      bufferPtr += n;
      return n;
   }

   @Override
   public int available() throws IOException {
      return (bufferLength - bufferPtr);
   }

   @Override
   public void close() throws IOException {
      disposeSasl();
      bufferPtr = 0;
      bufferLength = 0;
      inStream.close();
   }

   @Override
   public boolean markSupported() {
      return false;
   }

   private int fillBuffer() throws IOException {
      byte[] saslToken;
      try {
         int length = inStream.readInt();
         saslToken = new byte[length];
         inStream.readFully(saslToken);
         buffer = saslClient.unwrap(saslToken, 0, length);
      } catch (EOFException e) {
         return -1;
      } catch (SaslException se) {
         try {
            disposeSasl();
         } catch (SaslException e) {
         }
         throw se;
      }
      bufferPtr = 0;
      bufferLength = buffer.length;
      return bufferLength;
   }


   private void disposeSasl() throws SaslException {
      if (saslClient != null) {
         saslClient.dispose();
      }
   }
}
