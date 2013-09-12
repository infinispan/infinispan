package org.infinispan.server.websocket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.transport.LifecycleChannelPipelineFactory;
import org.infinispan.server.websocket.configuration.WebSocketServerConfiguration;
import org.infinispan.server.websocket.handlers.GetHandler;
import org.infinispan.server.websocket.handlers.NotifyHandler;
import org.infinispan.server.websocket.handlers.PutHandler;
import org.infinispan.server.websocket.handlers.RemoveHandler;
import org.infinispan.commons.util.CollectionFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * An HTTP server which serves Web Socket requests on an Infinispan cacheManager.
 * <p/>
 * Websocket specific code lifted from Netty WebSocket Server example.
 */
public class WebSocketServer extends AbstractProtocolServer {

   public static final String INFINISPAN_WS_JS_FILENAME = "infinispan-ws.js";

   private static String javascript;
   private WebSocketServerConfiguration configuration;

   public WebSocketServer() {
      super("WebSocket");
   }

   public OneToOneEncoder getEncoder() {
      return null;
   }

   public ChannelHandler getDecoder() {
      return null;
   }

   public void startInternal(ProtocolServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      this.configuration = (WebSocketServerConfiguration) configuration;
      super.startInternal(configuration, cacheManager);
   }

   @Override
   public LifecycleChannelPipelineFactory getPipeline() {
      return new WebSocketServerPipelineFactory(cacheManager());
   }

   private static class WebSocketServerPipelineFactory extends LifecycleChannelPipelineFactory {

      private CacheContainer cacheContainer;
      private Map<String, OpHandler> operationHandlers;
      private Map<String, Cache> startedCaches = CollectionFactory.makeConcurrentMap();

      public WebSocketServerPipelineFactory(CacheContainer cacheContainer) {
         this.cacheContainer = cacheContainer;

         operationHandlers = new HashMap<String, OpHandler>();
         operationHandlers.put("put", new PutHandler());
         operationHandlers.put("get", new GetHandler());
         operationHandlers.put("remove", new RemoveHandler());
         NotifyHandler notifyHandler = new NotifyHandler();
         operationHandlers.put("notify", notifyHandler);
         operationHandlers.put("unnotify", notifyHandler);
      }

      @Override
      public ChannelPipeline getPipeline() throws Exception {
         // Create a default pipeline implementation.
         ChannelPipeline pipeline = Channels.pipeline();

         pipeline.addLast("decoder", new HttpRequestDecoder());
         pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
         pipeline.addLast("encoder", new HttpResponseEncoder());
         pipeline.addLast("handler", new WebSocketServerHandler(cacheContainer, operationHandlers, startedCaches));

         return pipeline;
      }

      @Override
      public void stop() {
         // NO OP
      }
   }

   public static String getJavascript() {
      if (javascript != null) {
         return javascript;
      }

      BufferedReader scriptReader = new BufferedReader(new InputStreamReader(WebSocketServer.class.getResourceAsStream(INFINISPAN_WS_JS_FILENAME)));

      try {
         StringWriter writer = new StringWriter();

         String line = scriptReader.readLine();
         while (line != null) {
            writer.write(line);
            writer.write('\n');
            line = scriptReader.readLine();
         }

         javascript = writer.toString();

         return javascript;
      } catch (IOException e) {
         throw new IllegalStateException("Unexpected exception while sending Websockets script to client.", e);
      } finally {
         try {
            scriptReader.close();
         } catch (IOException e) {
            throw new IllegalStateException("Unexpected exception while closing Websockets script to client.", e);
         }
      }
   }
}
