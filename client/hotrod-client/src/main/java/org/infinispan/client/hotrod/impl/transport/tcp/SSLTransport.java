package org.infinispan.client.hotrod.impl.transport.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Transport implementation based on TCP using {@link SSLEngine} to encrypt the data transferred over network.
 *
 * @author rvansa@redhat.com
 */
public class SSLTransport extends TcpTransport {
   private static final Log log = LogFactory.getLog(SSLTransport.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private SSLEngine engine;
   private ByteBuffer netOutBuffer;
   private ByteBuffer netInBuffer;
   private ByteBuffer writeBuffer;

   public SSLTransport(SocketAddress serverAddress, TcpTransportFactory transportFactory) {
      super(serverAddress, transportFactory);
      if (serverAddress instanceof InetSocketAddress) {
         InetSocketAddress isa = (InetSocketAddress) serverAddress;
         engine = transportFactory.getSSLContext().createSSLEngine(isa.getHostName(), isa.getPort());
      } else {
         engine = transportFactory.getSSLContext().createSSLEngine();
      }
      engine.setUseClientMode(true);
   }

   @Override
   protected InputStream createInputStream() {
      return new SecureInputStream();
   }

   @Override
   public void flushWrite(ByteBuffer buffer) {
      if (trace) {
         log.tracef("Flushing %d bytes", buffer.limit());
      }
      writeBuffer = buffer;
      netOutBuffer = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
      netInBuffer = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
      netInBuffer.limit(0);
      try {
         engine.beginHandshake();
         checkHandshake(engine.getHandshakeStatus(), false);
      } catch (SSLException e) {
         throw new TransportException(e, getServerAddress());
      }
   }

   private void checkHandshake(SSLEngineResult.HandshakeStatus hs, boolean sync) throws SSLException {
      SSLEngineResult result = null;
      for (;;) {
         if (trace) {
            log.tracef("Handshake status is %s", hs);
         }
         switch (hs) {
            case NOT_HANDSHAKING:
            case FINISHED:
               result = continueTransport(sync);
               break;
            case NEED_TASK:
               if (sync) {
                  Runnable task;
                  while ((task = engine.getDelegatedTask()) != null) {
                     task.run();
                  }
               } else {
                  runDelegatedTasks();
               }
               return;
            case NEED_WRAP:
               result = wrapAppData(sync);
               break;
            case NEED_UNWRAP:
               result = unwrapAppData(sync);
               break;
         }
         if (result == null) return;
         hs = result.getHandshakeStatus();
      }
   }

   private void runDelegatedTasks() throws SSLException {
      Runnable task;
      final AtomicInteger counter = new AtomicInteger(Integer.MAX_VALUE);
      int tasks = 0;
      while ((task = engine.getDelegatedTask()) != null) {
         final Runnable rt = task;
         ++tasks;
         getTransportFactory().getAsyncExecutorService().execute(new Runnable() {
            @Override
            public void run() {
               rt.run();
               if (counter.decrementAndGet() == 0) {
                  try {
                     continueTransport(false);
                  } catch (SSLException e) {
                     futureThrow(e);
                  }
               }
            }
         });
      }
      if (trace) {
         log.tracef("Scheduled %d tasks", tasks);
      }
      // if not all scheduled tasks have been finished, return - others will do
      if (counter.addAndGet(tasks - Integer.MAX_VALUE) == 0) {
         continueTransport(false);
      }
   }

   private SSLEngineResult wrapAppData(boolean sync) throws SSLException {
      if (trace) {
         log.tracef("Wrapping [%d, %d] -> [%d, %d]",
               writeBuffer.position(), writeBuffer.limit(), netOutBuffer.position(), netOutBuffer.limit());
      }

      SSLEngineResult result = engine.wrap(writeBuffer, netOutBuffer);

      if (trace) {
         log.tracef("Wrapped with %s [%d, %d] -> [%d, %d]", result,
               writeBuffer.position(), writeBuffer.limit(), netOutBuffer.position(), netOutBuffer.limit());
      }
      switch (result.getStatus()) {
         case BUFFER_UNDERFLOW:
            throw new IllegalStateException("Sorry, no more data");
         case BUFFER_OVERFLOW:
            netOutBuffer = ByteBuffer.allocate(netOutBuffer.capacity() + engine.getSession().getPacketBufferSize());
            return result;
         case OK:
            break;
         case CLOSED:
            throw new TransportException("Connection was closed.", serverAddress);
      }
      netOutBuffer.flip();
      if (sync) {
         try {
            socketChannel.write(netOutBuffer).get(getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException(e, serverAddress);
         } catch (Exception e) {
            throw new TransportException(e, serverAddress);
         }
         return result;
      } else {
         socketChannel.write(netOutBuffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, this, new WriteCompletionHandler());
         return null;
      }
   }

   private SSLEngineResult unwrapAppData(boolean sync) throws SSLException {
      if (trace) {
         log.tracef("Unwrapping [%d, %d] - > [%d, %d]",
               netInBuffer.position(), netInBuffer.limit(), readBuffer.position(), readBuffer.limit());
      }

      SSLEngineResult result = engine.unwrap(netInBuffer, readBuffer);

      if (trace) {
         log.tracef("Unwrapped with %s [%d, %d] - > [%d, %d]", result,
               netInBuffer.position(), netInBuffer.limit(), readBuffer.position(), readBuffer.limit());
      }
      switch (result.getStatus()) {
         case BUFFER_UNDERFLOW:
            netInBuffer.compact();
            if (sync) {
               try {
                  socketChannel.read(netInBuffer).get(getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new TransportException(e, serverAddress);
               } catch (Exception e) {
                  throw new TransportException(e, serverAddress);
               }
            } else {
               socketChannel.read(netInBuffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, this, new ReadCompletionHandler());
               return null;
            }
         case BUFFER_OVERFLOW:
            ByteBuffer bigger = ByteBuffer.allocate(readBuffer.capacity() + engine.getSession().getApplicationBufferSize());
            readBuffer.flip();
            bigger.put(readBuffer);
            readBuffer = bigger;
            break;
         case OK:
            break;
         case CLOSED:
            throw new TransportException("Connection was closed.", serverAddress);
      }
      return result;
   }

   private SSLEngineResult continueTransport(boolean sync) throws SSLException {
      log.trace("Continuing");
      if (writeBuffer.hasRemaining()) {
         SSLEngineResult result = wrapAppData(sync);
         return result;
      } else if (readBuffer.position() > 0) {
         readBuffer.flip();
         try {
            Object value = responseReader.call();
            futureSet(value);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            futureThrow(new TransportException(e, serverAddress));
         } catch (Exception e) {
            futureThrow(e);
         } finally {
            readBuffer.compact();
            writeBuffer.compact();
         }
         return null;
      } else {
         netInBuffer.compact();
         socketChannel.read(netInBuffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, this, new ReadCompletionHandler());
         return null;
      }
   }

   private class ReadCompletionHandler implements CompletionHandler<Integer, SSLTransport> {
      @Override
      public void completed(Integer result, SSLTransport attachment) {
         if (trace) {
            log.tracef("Read completed with result %d", result);
         }
         if (result < 0) {
            invalid = true;
            finishFuture.notifyException(new TransportException("End of stream reached!", serverAddress));
         } else {
            netInBuffer.flip();
            try {
               SSLEngineResult sslresult = unwrapAppData(false);
               if (sslresult != null) {
                  checkHandshake(sslresult.getHandshakeStatus(), false);
               }
            } catch (SSLException e) {
               futureThrow(e);
            }
         }
      }

      @Override
      public void failed(Throwable exc, SSLTransport attachment) {
         futureThrow(exc);
      }
   }

   private class WriteCompletionHandler implements CompletionHandler<Integer, SSLTransport> {
      @Override
      public void completed(Integer result, SSLTransport attachment) {
         if (trace) {
            log.tracef("Write completed with result %d [%d, %d]", result, netOutBuffer.position(), netOutBuffer.limit());
         }
         if (netOutBuffer.hasRemaining()) {
            socketChannel.write(netOutBuffer, getTransportFactory().getSoTimeout(), TimeUnit.MILLISECONDS, SSLTransport.this, this);
         } else {
            netOutBuffer.clear();
            try {
               checkHandshake(engine.getHandshakeStatus(), false);
            } catch (SSLException e) {
               futureThrow(e);
            }
         }
      }

      @Override
      public void failed(Throwable exc, SSLTransport attachment) {
         futureThrow(exc);
      }
   }

   private class SecureInputStream extends InputStream {
      @Override
      public int read() throws IOException {
         if (!readBuffer.hasRemaining()) {
            // when we run out of buffer, synchronously reads
            readBuffer.clear();
            try {
               do {
                  checkHandshake(SSLEngineResult.HandshakeStatus.NEED_UNWRAP, true);
               } while (readBuffer.position() == 0);
            } catch (Exception e) {
               invalid = true;
               throw new TransportException(e, serverAddress);
            }
            readBuffer.flip();
         }
         return readBuffer.get() & 0xff;
      }
   }
}
