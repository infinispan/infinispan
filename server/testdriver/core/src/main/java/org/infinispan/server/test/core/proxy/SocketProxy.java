package org.infinispan.server.test.core.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;

/**
 * A generic socket proxy. This is used to overcome a design choice in testcontainers which doesn't allow mapping
 * container ports to a known port on the host (to avoid clashes).
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class SocketProxy implements AutoCloseable {
   static final Log log = LogFactory.getLog(SocketProxy.class);
   final InetAddress bindAddress;
   final int localPort;
   final InetAddress remoteAddress;
   final int remotePort;
   final CountDownLatch latch;
   private ServerSocket serverSocket;

   public SocketProxy(InetAddress bindAddress, int localPort, InetAddress remoteAddress, int remotePort) {
      this.bindAddress = bindAddress;
      this.localPort = localPort;
      this.remoteAddress = remoteAddress;
      this.remotePort = remotePort;
      this.latch = new CountDownLatch(2);
      start();
   }

   public void start() {
      try {
         serverSocket = new ServerSocket(localPort, 10, bindAddress);
         serverSocket.setSoTimeout(5000);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      new Thread(() -> {
         while (latch.getCount() > 1) {
            try (Socket localSocket = serverSocket.accept(); Socket remoteSocket = new Socket(remoteAddress, remotePort);) {
               remoteSocket.setSoTimeout(20000);
               new Thread(() -> {
                  // remote -> local
                  try (InputStream remoteSocketInputStream = remoteSocket.getInputStream(); OutputStream localSocketOutputStream = localSocket.getOutputStream()) {
                     log.debug("remote->local: opened");
                     byte[] remoteBuffer = new byte[4096];
                     int remoteBytesRead;
                     while ((remoteBytesRead = remoteSocketInputStream.read(remoteBuffer)) != -1) {
                        localSocketOutputStream.write(remoteBuffer, 0, remoteBytesRead);
                        localSocketOutputStream.flush();
                        log.debugf("remote->local: transferred %d bytes\n", remoteBytesRead);
                     }
                  } catch (IOException e) {
                     log.error("remote->local", e);
                  }
               }).start();

               // local -> remote
               try (InputStream localSocketInputStream = localSocket.getInputStream(); OutputStream remoteSocketOutputStream = remoteSocket.getOutputStream()) {
                  log.debug("local->remote: opened");
                  byte[] localBuffer = new byte[4096];
                  int localBytesRead;
                  while ((localBytesRead = localSocketInputStream.read(localBuffer)) != -1) {
                     remoteSocketOutputStream.write(localBuffer, 0, localBytesRead);
                     remoteSocketOutputStream.flush();
                     log.debugf("local->remote: transferred %d bytes\n", localBytesRead);
                  }
               } catch (IOException e) {
                  log.error("local->remote", e);
               } finally {
                  Util.close(remoteSocket);
               }
            } catch (IOException e) {
               log.error("connect", e);
            }
         }
         latch.countDown();
      }).start();
   }

   @Override
   public void close() throws Exception {
      latch.countDown();
      latch.await(5000, TimeUnit.MILLISECONDS);
      Util.close(serverSocket);
   }

   @Override
   public String toString() {
      return "SocketProxy{" +
            "bindAddress=" + bindAddress +
            ", localPort=" + localPort +
            ", remoteAddress=" + remoteAddress +
            ", remotePort=" + remotePort +
            '}';
   }
}
