package org.infinispan.server.core.transport;

import java.net.SocketAddress;
import java.time.Instant;

import javax.security.auth.Subject;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * @since 15.0
 **/
public class ConnectionMetadata {
   static final AttributeKey<ConnectionMetadata> METADATA = AttributeKey.newInstance("METADATA");
   private final Channel channel;
   private long id;
   private Subject subject;
   private String clientName;
   private String clientLibName;
   private String clientLibVersion;
   private String protocolVersion;
   private Instant created;

   public static ConnectionMetadata getInstance(Channel channel) {
      ConnectionMetadata existing = channel.attr(METADATA).get();
      if (existing == null) {
         ConnectionMetadata metadata = new ConnectionMetadata(channel);
         existing = channel.attr(METADATA).setIfAbsent(metadata);
         return existing == null ? metadata : existing;
      } else {
         return existing;
      }
   }

   private ConnectionMetadata(Channel channel) {
      this.channel = channel;
   }

   public void id(long id) {
      this.id = id;
   }

   public long id() {
      return id;
   }

   public Subject subject() {
      return subject;
   }

   public SocketAddress localAddress() {
      return channel.localAddress();
   }

   public SocketAddress remoteAddress() {
      return channel.remoteAddress();
   }

   public String clientName() {
      return clientName;
   }

   public String clientLibraryName() {
      return clientLibName;
   }

   public String clientLibraryVersion() {
      return clientLibVersion;
   }

   public String protocolVersion() {
      return protocolVersion;
   }

   public void clientLibraryName(String name) {
      this.clientLibName = name;
   }

   public void clientLibraryVersion(String version) {
      this.clientLibVersion = version;
   }

   public void clientName(String name) {
      this.clientName = name;
   }

   public void created(Instant timestamp) {
      this.created = timestamp;
   }

   public Instant created() {
      return created;
   }

   public void subject(Subject subject) {
      this.subject = subject;
   }

   public void protocolVersion(String version) {
      this.protocolVersion = version;
   }
}
