package org.infinispan.client.hotrod.impl.transport.tcp;

import static org.infinispan.commons.io.UnsignedNumeric.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.AbstractTransport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.commons.util.concurrent.SettableFuture;

/**
 * Transport implementation based on TCP.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpTransport extends AbstractTransport {

   //needed for debugging
   private static AtomicLong ID_COUNTER = new AtomicLong(0);

   private static final Log log = LogFactory.getLog(TcpTransport.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   //private final Socket socket;
   //private final SocketChannel socketChannel;
   AsynchronousSocketChannel socketChannel;

   private InputStream inputStream;
   private OutputStream outputStream;
   protected ByteBufferOutputStream byteBufferOutputStream;
   protected ByteBuffer readBuffer;


   protected final SocketAddress serverAddress;
   private final long id = ID_COUNTER.incrementAndGet();

   protected volatile boolean invalid;

   //debug
   protected AtomicReference<Thread> flusher = new AtomicReference<Thread>();
   protected AtomicLong useCounter = new AtomicLong();

   protected volatile Callable responseReader;
   protected volatile NotifyingFutureImpl finishFuture;
   protected final Future<Void> connectFuture;
   //private SaslClient saslClient;


   public TcpTransport(SocketAddress serverAddress, TcpTransportFactory transportFactory) {
      super(transportFactory);
      this.serverAddress = serverAddress;
      try {
         socketChannel = AsynchronousSocketChannel.open(transportFactory.getAsynchronousChannelGroup());
         connectFuture = socketChannel.connect(serverAddress);
         socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, transportFactory.isTcpNoDelay());
         outputStream = byteBufferOutputStream =
               new ByteBufferOutputStream(ByteBuffer.allocate(socketChannel.getOption(StandardSocketOptions.SO_SNDBUF)));
         readBuffer = ByteBuffer.allocate(socketChannel.getOption(StandardSocketOptions.SO_RCVBUF));
         readBuffer.limit(0);
         inputStream = createInputStream();
      } catch (Exception e) {
         String message = String.format("Could not connect to server: %s", serverAddress);
         log.trace(message, e);
         if (socketChannel != null) {
            try {
               socketChannel.close();
            } catch (IOException err) {
               log.errorClosingSocket(this, err);
            }
         }
         throw new TransportException(message, e, serverAddress);
      }
   }

   protected InputStream createInputStream() {
      return new TransportInputStream();
   }

   /*void setSaslClient(SaslClient saslClient) {
      this.saslClient = saslClient;
      try {
         this.inputStream = new SaslInputStream(socket.getInputStream(), saslClient);
         this.outputStream = new SaslOutputStream(socket.getOutputStream(), saslClient);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }*/

   @Override
   public void writeVInt(int vInt) {
      try {
         if (trace)
            log.tracef("Writing varint %d (%d bytes)", vInt, vInt == 0 ? 1 : (31 - Integer.numberOfLeadingZeros(vInt)) / 7 + 1);
         writeUnsignedInt(outputStream, vInt);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public void writeVLong(long l) {
      try {
         if (trace)
            log.tracef("Writing varlong %d (%d bytes)", l, l == 0 ? 1 : (63 - Long.numberOfLeadingZeros(l)) / 7 + 1);
         writeUnsignedLong(outputStream, l);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public long readVLong() {
      try {
         return readUnsignedLong(inputStream);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   public int readVInt() {
      try {
         return readUnsignedInt(inputStream);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
   }

   @Override
   protected void writeBytes(byte[] toAppend) {
      try {
         outputStream.write(toAppend);
         if (trace) {
            log.tracef("Wrote %d bytes", toAppend.length);
         }
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(
               "Problems writing data to stream", e, serverAddress);
      }
   }

   @Override
   public void writeByte(short toWrite) {
      try {
         outputStream.write(toWrite);
         if (trace) {
            log.tracef("Wrote byte %d", toWrite);
         }

      } catch (IOException e) {
         invalid = true;
         throw new TransportException(
               "Problems writing data to stream", e, serverAddress);
      }
   }

   protected void futureSet(Object value) throws InterruptedException {
      Thread flusher = this.flusher.getAndSet(null);
      log.warn("OK start " + useCounter.get());
      // we have to first notify listeners and only then set the actual future,
      // otherwise in the NotifyingFutureImpl the future.get() could unblock
      // but actualReturnValue wouldn't be set yet
      finishFuture.notifyDone(value);
      ((SettableFuture) finishFuture.getFuture()).set(value);
      log.warn("OK " + value + " " + this + " " + flusher);
   }

   protected void futureThrow(Throwable exc) {
      Thread flusher = this.flusher.getAndSet(null);
      log.warn("Problem start " + useCounter.get());
      try {
         if (exc instanceof InterruptedByTimeoutException) {
            log.trace("Translating InterruptedByTimeoutException -> SocketTimeoutException", exc);
            exc = new SocketTimeoutException(exc.getMessage());
         }
         TransportException te = exc instanceof TransportException ?
               (TransportException) exc : new TransportException(exc, serverAddress);
         ((SettableFuture) finishFuture.getFuture()).setThrowable(te);
         finishFuture.notifyException(te);
      } catch (InterruptedException e) {
         throw new IllegalStateException("Future is always set before this is called", e);
      } finally {
         log.warn("Problem " + this + " " + flusher, exc);
      }
   }

   private class WriteHandler<T> implements CompletionHandler<Integer, TcpTransport> {
      @Override
      public void completed(Integer result, TcpTransport attachment) {
         ByteBuffer buffer = byteBufferOutputStream.getBuffer();
         log.tracef("Write completed %d, buffer is [%d %d]", result, buffer.position(), buffer.limit());
         if (buffer.hasRemaining()) {
            socketChannel.write(buffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, TcpTransport.this, this);
         } else {
            buffer.clear(); // prepare the write buffer for future use
            readBuffer.clear(); // prepare the read buffer for current use
            socketChannel.read(readBuffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, TcpTransport.this, new ReadHandler());
         }
      }

      @Override
      public void failed(Throwable exc, TcpTransport attachment) {
         try {
            futureThrow(exc);
         } finally {
            byteBufferOutputStream.getBuffer().clear();
         }
      }
   }

   private class ReadHandler<T> implements CompletionHandler<Integer, TcpTransport> {
      @Override
      public void completed(Integer result, TcpTransport attachment) {
         if (result < 0) {
            invalid = true;
            futureThrow(new TransportException("End of stream reached!", serverAddress));
         } else {
            readBuffer.flip();
            try {
               Object value = responseReader.call();
               futureSet(value);
            } catch (Throwable t) {
               if (t instanceof InterruptedException) Thread.currentThread().interrupt();
               futureThrow(t);
            }
         }
      }

      @Override
      public void failed(Throwable exc, TcpTransport attachment) {
         futureThrow(exc);
      }
   }

   private class TransportInputStream extends InputStream {
      @Override
      public int read() throws IOException {
         if (!readBuffer.hasRemaining()) {
            // when we run out of buffer, synchronously reads
            readBuffer.clear();
            try {
               int read = socketChannel.read(readBuffer).get(getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS);
               if (read < 0) {
                  invalid = true;
                  return -1;
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               invalid = true;
               throw new TransportException(e, serverAddress);
            } catch (Exception e) {
               invalid = true;
               throw new TransportException(e, serverAddress);
            }
            readBuffer.flip();
         }
         return readBuffer.get() & 0xff;
      }
   }

   @Override
   public <T> NotifyingFuture<T> flush(Callable<T> callable) {
      Thread other = flusher.getAndSet(Thread.currentThread());
      if (other != null) {
         throw new IllegalStateException(this + " = " + System.identityHashCode(this) + " used by " + other + ", this is " + Thread.currentThread());
      }
      log.tracef("Flush %d", useCounter.incrementAndGet());
      this.responseReader = callable;
      this.finishFuture = new NotifyingFutureImpl<T>();
      finishFuture.setFuture(new SettableFuture<T>());
      final ByteBuffer buffer = byteBufferOutputStream.getBuffer();
      buffer.flip();
      if (connectFuture.isDone()) {
         if (connectFuture.isCancelled()) {
            throw new TransportException("Connection was cancelled!", serverAddress);
         }
         try {
            connectFuture.get();
         } catch (InterruptedException e) {
            throw new IllegalStateException("Cannot be interrupted when the future is done.", e);
         } catch (ExecutionException e) {
            String message = String.format("Could not connect to server: %s", serverAddress);
            log.trace(message, e.getCause());
            throw new TransportException(message, e.getCause(), serverAddress);
         }
         log.trace("Transport is connected, flushing in the same thread");
         flushWrite(buffer);
      } else {
         final long connectionStart = System.currentTimeMillis();
         log.trace("Waiting for connection to finish, delegating flush to async executor service");
         getTransportFactory().getAsyncExecutorService().execute(new Runnable() {
            @Override
            public void run() {
               try {
                  final long now = System.currentTimeMillis();
                  if (now > connectionStart + getTransportFactory().getConnectTimeout()) {
                     String message = String.format("Could not connect to server: %s", serverAddress);
                     log.trace(message);
                     futureThrow(new TransportException(message, serverAddress));
                  }
                  try {
                     connectFuture.get(0, TimeUnit.MILLISECONDS);
                  } catch (TimeoutException e) {
                     log.trace("Not connected yet, requesting repeated execution");
                     getTransportFactory().getAsyncExecutorService().execute(this);
                     return;
                  } catch (ExecutionException e) {
                     throw e.getCause(); // unwrap and catch below
                  }
                  log.trace("Flushing from async executor service");
                  flushWrite(buffer);
               } catch (Throwable e) {
                  if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                  String message = String.format("Could not connect to server: %s", serverAddress);
                  log.trace(message, e);
                  futureThrow(new TransportException(message, e, serverAddress));
               }
            }
         });
      }
      return finishFuture;
   }

   protected void flushWrite(ByteBuffer buffer) {
      log.tracef("Flushing buffer [%d %d]", buffer.position(), buffer.limit());
      socketChannel.write(buffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, this, new WriteHandler());
   }

   @Override
   public short readByte() {
      int resultInt;
      try {
         resultInt = inputStream.read();
         if (trace)
            log.tracef("Read byte %d from %s", resultInt, serverAddress);
      } catch (IOException e) {
         invalid = true;
         throw new TransportException(e, serverAddress);
      }
      if (resultInt == -1) {
         throw new TransportException("End of stream reached!", serverAddress);
      }
      return (short) resultInt;
   }

   @Override
   public void release() {
      try {
         socketChannel.close();
      } catch (IOException e) {
         invalid = true;
         log.errorClosingSocket(this, e);
      }
   }

   @Override
   public byte[] readByteArray(final int size) {
      byte[] result = new byte[size];
      boolean done = false;
      int offset = 0;
      do {
         int read;
         try {
            int len = size - offset;
            if (trace) {
               log.tracef("Offset: %d, len=%d, size=%d", offset, len, size);
            }
            read = inputStream.read(result, offset, len);
         } catch (IOException e) {
            invalid = true;
            throw new TransportException(e, serverAddress);
         }
         if (read == -1) {
            throw new RuntimeException("End of stream reached!");
         }
         if (read + offset == size) {
            done = true;
         } else {
            offset += read;
            if (offset > result.length) {
               throw new IllegalStateException("Assertion!");
            }
         }
      } while (!done);
      if (trace) {
         log.tracef("Successfully read array with size: %d", size);
      }
      return result;
   }

   public SocketAddress getServerAddress() {
      return serverAddress;
   }

   @Override
   public String toString() {
      SocketAddress localAddress;
      try {
         localAddress = socketChannel.getLocalAddress();
      } catch (IOException e) {
         localAddress = null;
      }
      return "TcpTransport{localAddress=" + localAddress +
              ", serverAddress=" + serverAddress +
              ", id =" + id +
              "} ";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      TcpTransport that = (TcpTransport) o;

      if (socketChannel != that.socketChannel) {
         return false;
      }
      if (serverAddress != null ? !serverAddress.equals(that.serverAddress) : that.serverAddress != null) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = socketChannel != null ? socketChannel.hashCode() : 0;
      result = 31 * result + (serverAddress != null ? serverAddress.hashCode() : 0);
      return result;
   }

   public void destroy() {
      try {
         if (inputStream != null) inputStream.close();
         if (outputStream != null) outputStream.close();
         if (socketChannel != null) socketChannel.close();
         if (trace) {
            log.tracef("Successfully closed socketChannel to %s", serverAddress);
         }
      } catch (IOException e) {
         invalid = true;
         log.errorClosingSocket(this, e);
         // Just in case an exception is thrown, make sure they're fully closed
         Util.close(inputStream, outputStream, socketChannel);
      }
   }

   public boolean isValid() {
      return socketChannel.isOpen() && !invalid;
   }

   public long getId() {
      return id;
   }

   @Override
   public byte[] dumpStream() {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
         // Read 32kb at most
         for (int i = 0; i < 32768; i++) {
            int b = inputStream.read();
            if (b < 0) {
               break;
            }
            os.write(b);
         }
      } catch (IOException e) {
         // Ignore
      } finally {
         try {
            socketChannel.close();
         } catch (IOException e) {
            // Ignore
         }
      }
      return os.toByteArray();
   }

   @Override
   public SocketAddress getRemoteSocketAddress() {
      try {
         return socketChannel.getRemoteAddress();
      } catch (IOException e) {
         log.debug("Failed to retrieve remote address");
         return null;
      }
   }

   @Override
   public void invalidate() {
      invalid = true;
   }
}
