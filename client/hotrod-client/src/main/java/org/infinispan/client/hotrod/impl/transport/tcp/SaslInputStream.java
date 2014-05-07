package org.infinispan.client.hotrod.impl.transport.tcp;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * SaslInputStream.
 *
 * Taken from the ASL 2.0-licensed Hadoop source code org.apache.hadoop.security.SaslInputStream.
 * The SaslServer stuff was removed as irrelevant to our use-case.
 *
 * @since 7.0
 */
public class SaslInputStream extends InputStream implements ReadableByteChannel {
   public static final Log LOG = LogFactory.getLog(SaslInputStream.class);

   private final DataInputStream inStream;
   /** Should we wrap the communication channel? */

   /*
    * data read from the underlying input stream before being processed by SASL
    */
   private byte[] saslToken;
   private final SaslClient saslClient;
   private final byte[] lengthBuf = new byte[4];
   /*
    * buffer holding data that have been processed by SASL, but have not been read out
    */
   private byte[] obuffer;
   // position of the next "new" byte
   private int ostart = 0;
   // position of the last "new" byte
   private int ofinish = 0;
   // whether or not this stream is open
   private boolean isOpen = true;

   private static int unsignedBytesToInt(byte[] buf) {
      if (buf.length != 4) {
         throw new IllegalArgumentException("Cannot handle byte array other than 4 bytes");
      }
      int result = 0;
      for (int i = 0; i < 4; i++) {
         result <<= 8;
         result |= ((int) buf[i] & 0xff);
      }
      return result;
   }

   /**
    * Read more data and get them processed <br>
    * Entry condition: ostart = ofinish <br>
    * Exit condition: ostart <= ofinish <br>
    *
    * return (ofinish-ostart) (we have this many bytes for you), 0 (no data now, but could have more
    * later), or -1 (absolutely no more data)
    */
   private int readMoreData() throws IOException {
      try {
         inStream.readFully(lengthBuf);
         int length = unsignedBytesToInt(lengthBuf);
         if (LOG.isDebugEnabled())
            LOG.debug("Actual length is " + length);
         saslToken = new byte[length];
         inStream.readFully(saslToken);
      } catch (EOFException e) {
         return -1;
      }
      try {
         obuffer = saslClient.unwrap(saslToken, 0, saslToken.length);
      } catch (SaslException se) {
         try {
            disposeSasl();
         } catch (SaslException ignored) {
         }
         throw se;
      }
      ostart = 0;
      if (obuffer == null)
         ofinish = 0;
      else
         ofinish = obuffer.length;
      return ofinish;
   }

   /**
    * Disposes of any system resources or security-sensitive information Sasl might be using.
    *
    * @exception SaslException
    *               if a SASL error occurs.
    */
   private void disposeSasl() throws SaslException {
      if (saslClient != null) {
         saslClient.dispose();
      }
   }

   /**
    * Constructs a SASLInputStream from an InputStream and a SaslClient <br>
    * Note: if the specified InputStream or SaslClient is null, a NullPointerException may be thrown
    * later when they are used.
    *
    * @param inStream
    *           the InputStream to be processed
    * @param saslClient
    *           an initialized SaslClient object
    */
   public SaslInputStream(InputStream inStream, SaslClient saslClient) {
      this.inStream = new DataInputStream(inStream);
      this.saslClient = saslClient;
   }

   /**
    * Reads the next byte of data from this input stream. The value byte is returned as an
    * <code>int</code> in the range <code>0</code> to <code>255</code>. If no byte is available
    * because the end of the stream has been reached, the value <code>-1</code> is returned. This
    * method blocks until input data is available, the end of the stream is detected, or an
    * exception is thrown.
    * <p>
    *
    * @return the next byte of data, or <code>-1</code> if the end of the stream is reached.
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public int read() throws IOException {
      if (ostart >= ofinish) {
         // we loop for new data as we are blocking
         int i = 0;
         while (i == 0)
            i = readMoreData();
         if (i == -1)
            return -1;
      }
      return ((int) obuffer[ostart++] & 0xff);
   }

   /**
    * Reads up to <code>b.length</code> bytes of data from this input stream into an array of bytes.
    * <p>
    * The <code>read</code> method of <code>InputStream</code> calls the <code>read</code> method of
    * three arguments with the arguments <code>b</code>, <code>0</code>, and <code>b.length</code>.
    *
    * @param b
    *           the buffer into which the data is read.
    * @return the total number of bytes read into the buffer, or <code>-1</code> is there is no more
    *         data because the end of the stream has been reached.
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
   }

   /**
    * Reads up to <code>len</code> bytes of data from this input stream into an array of bytes. This
    * method blocks until some input is available. If the first argument is <code>null,</code> up to
    * <code>len</code> bytes are read and discarded.
    *
    * @param b
    *           the buffer into which the data is read.
    * @param off
    *           the start offset of the data.
    * @param len
    *           the maximum number of bytes read.
    * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more
    *         data because the end of the stream has been reached.
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      if (ostart >= ofinish) {
         // we loop for new data as we are blocking
         int i = 0;
         while (i == 0)
            i = readMoreData();
         if (i == -1)
            return -1;
      }
      if (len <= 0) {
         return 0;
      }
      int available = ofinish - ostart;
      if (len < available)
         available = len;
      if (b != null) {
         System.arraycopy(obuffer, ostart, b, off, available);
      }
      ostart = ostart + available;
      return available;
   }

   /**
    * Skips <code>n</code> bytes of input from the bytes that can be read from this input stream
    * without blocking.
    *
    * <p>
    * Fewer bytes than requested might be skipped. The actual number of bytes skipped is equal to
    * <code>n</code> or the result of a call to {@link #available() <code>available</code>},
    * whichever is smaller. If <code>n</code> is less than zero, no bytes are skipped.
    *
    * <p>
    * The actual number of bytes skipped is returned.
    *
    * @param n
    *           the number of bytes to be skipped.
    * @return the actual number of bytes skipped.
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public long skip(long n) throws IOException {
      int available = ofinish - ostart;
      if (n > available) {
         n = available;
      }
      if (n < 0) {
         return 0;
      }
      ostart += n;
      return n;
   }

   /**
    * Returns the number of bytes that can be read from this input stream without blocking. The
    * <code>available</code> method of <code>InputStream</code> returns <code>0</code>. This method
    * <B>should</B> be overridden by subclasses.
    *
    * @return the number of bytes that can be read from this input stream without blocking.
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public int available() throws IOException {
      return (ofinish - ostart);
   }

   /**
    * Closes this input stream and releases any system resources associated with the stream.
    * <p>
    * The <code>close</code> method of <code>SASLInputStream</code> calls the <code>close</code>
    * method of its underlying input stream.
    *
    * @exception IOException
    *               if an I/O error occurs.
    */
   @Override
   public void close() throws IOException {
      disposeSasl();
      ostart = 0;
      ofinish = 0;
      inStream.close();
      isOpen = false;
   }

   /**
    * Tests if this input stream supports the <code>mark</code> and <code>reset</code> methods,
    * which it does not.
    *
    * @return <code>false</code>, since this class does not support the <code>mark</code> and
    *         <code>reset</code> methods.
    */
   @Override
   public boolean markSupported() {
      return false;
   }

   @Override
   public boolean isOpen() {
      return isOpen;
   }

   @Override
   public int read(ByteBuffer dst) throws IOException {
      int bytesRead = 0;
      if (dst.hasArray()) {
         bytesRead = read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
         if (bytesRead > -1) {
            dst.position(dst.position() + bytesRead);
         }
      } else {
         byte[] buf = new byte[dst.remaining()];
         bytesRead = read(buf);
         if (bytesRead > -1) {
            dst.put(buf, 0, bytesRead);
         }
      }
      return bytesRead;
   }
}
