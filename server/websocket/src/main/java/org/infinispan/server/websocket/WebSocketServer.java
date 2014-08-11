package org.infinispan.server.websocket;

import io.netty.channel.*;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.infinispan.Cache;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.websocket.handlers.GetHandler;
import org.infinispan.server.websocket.handlers.NotifyHandler;
import org.infinispan.server.websocket.handlers.PutHandler;
import org.infinispan.server.websocket.handlers.RemoveHandler;
import org.infinispan.server.websocket.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

/**
 * An HTTP server which serves Web Socket requests on an Infinispan cacheManager.
 * <p>
 *    Websocket specific code lifted from Netty WebSocket Server example.
 * </p>
 */
public class WebSocketServer extends AbstractProtocolServer {

   private static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   public static final String INFINISPAN_WS_JS_FILENAME = "infinispan-ws.js";

   private static String javascript;

   public WebSocketServer() {
      super("WebSocket");
   }

   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   public ChannelInboundHandler getDecoder() {
      return null;
   }

   public void startInternal(ProtocolServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      super.startInternal(configuration, cacheManager);
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return new WebSocketServerPipelineFactory(cacheManager());
   }

   private static class WebSocketServerPipelineFactory extends ChannelInitializer<Channel> {

      private CacheContainer cacheContainer;
      private Map<String, OpHandler> operationHandlers;
      private Map<String, Cache<Object, Object>> startedCaches = CollectionFactory.makeConcurrentMap();

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
      public void initChannel(Channel channel) throws Exception {
         // Create a default pipeline implementation.
         ChannelPipeline pipeline = channel.pipeline();

         pipeline.addLast("decoder", new HttpRequestDecoder());
         pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
         pipeline.addLast("encoder", new HttpResponseEncoder());
         pipeline.addLast("handler", new WebSocketServerHandler(cacheContainer, operationHandlers, startedCaches));
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
         throw logger.unableToSendWebSocketsScriptToTheClient(e);
      } finally {
         try {
            scriptReader.close();
         } catch (IOException e) {
            throw logger.unableToCloseWebSocketsStream(e);
         }
      }
   }
}
