package org.infinispan.server.core.configuration;

/**
 * ServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public abstract class ProtocolServerConfiguration {
   private final String defaultCacheName;
   private final String name;
   private final String host;
   private final int port;
   private final int idleTimeout;
   private final int recvBufSize;
   private final int sendBufSize;
   private final SslConfiguration ssl;
   private final boolean tcpNoDelay;
   private final int workerThreads;

   protected ProtocolServerConfiguration(String defaultCacheName, String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      this.defaultCacheName = defaultCacheName;
      this.name = name;
      this.host = host;
      this.port = port;
      this.idleTimeout = idleTimeout;
      this.recvBufSize = recvBufSize;
      this.sendBufSize = sendBufSize;
      this.ssl = ssl;
      this.tcpNoDelay = tcpNoDelay;
      this.workerThreads = workerThreads;
   }

   public String defaultCacheName() {
      return defaultCacheName;
   }

   public String name() {
      return name;
   }

   public String host() {
      return host;
   }

   public int port() {
      return port;
   }

   public int idleTimeout() {
      return idleTimeout;
   }

   public int recvBufSize() {
      return recvBufSize;
   }

   public int sendBufSize() {
      return sendBufSize;
   }

   public SslConfiguration ssl() {
      return ssl;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay;
   }

   public int workerThreads() {
      return workerThreads;
   }

   @Override
   public String toString() {
      return "ProtocolServerConfiguration [defaultCacheName=" + defaultCacheName +", name=" + name + ", host=" + host + ", port=" + port + ", idleTimeout=" + idleTimeout + ", recvBufSize=" + recvBufSize + ", sendBufSize="
            + sendBufSize + ", ssl=" + ssl + ", tcpNoDelay=" + tcpNoDelay + ", workerThreads=" + workerThreads + "]";
   }

}
