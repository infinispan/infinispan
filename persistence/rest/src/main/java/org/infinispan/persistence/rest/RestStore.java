package org.infinispan.persistence.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.Util;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.rest.configuration.RestStoreConfiguration;
import org.infinispan.persistence.rest.logging.Log;
import org.infinispan.persistence.rest.metadata.MetadataHelper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import net.jcip.annotations.ThreadSafe;

/**
 * RestStore.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Store(shared = true)
@ThreadSafe
@ConfiguredBy(RestStoreConfiguration.class)
public class RestStore implements AdvancedLoadWriteStore {
   private static final String MAX_IDLE_TIME_SECONDS = "maxIdleTimeSeconds";
   private static final String TIME_TO_LIVE_SECONDS = "timeToLiveSeconds";
   private static final Log log = LogFactory.getLog(RestStore.class, Log.class);
   private volatile RestStoreConfiguration configuration;
   private Bootstrap bootstrap;
   private InternalEntryFactory iceFactory;
   private MarshallingTwoWayKey2StringMapper key2StringMapper;
   private String path;
   private MetadataHelper metadataHelper;
   private final URLCodec urlCodec = new URLCodec();
   private InitializationContext ctx;

   private EventLoopGroup workerGroup;

   private int maxContentLength;


   @Override
   public void init(InitializationContext initializationContext) {
      configuration = initializationContext.getConfiguration();
      ctx = initializationContext;
   }

   @Override
   public void start() {
      if (iceFactory == null) {
         iceFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }

      ConnectionPoolConfiguration pool = configuration.connectionPool();
      workerGroup = new NioEventLoopGroup();
      Bootstrap b = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class);
      b.handler(new ChannelInitializer<SocketChannel>() {
         @Override
         protected void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(new HttpClientCodec());
         }
      });
      b.option(ChannelOption.SO_KEEPALIVE, true); // TODO make this part of configuration options
      b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, pool.connectionTimeout());
      b.option(ChannelOption.SO_SNDBUF, pool.bufferSize());// TODO make sure this is appropriate
      b.option(ChannelOption.SO_RCVBUF, pool.bufferSize());
      b.option(ChannelOption.TCP_NODELAY, pool.tcpNoDelay());
      bootstrap = b;
      maxContentLength = configuration.maxContentLength();

      this.key2StringMapper = Util.getInstance(configuration.key2StringMapper(), ctx.getCache().getAdvancedCache().getClassLoader());
      this.key2StringMapper.setMarshaller(ctx.getMarshaller());
      this.path = configuration.path();
      try {
         if (configuration.appendCacheNameToPath()) {
            path = path + urlCodec.encode(ctx.getCache().getName()) + "/";
         }
      } catch (EncoderException e) {
      }
      this.metadataHelper = Util.getInstance(configuration.metadataHelper(), ctx.getCache().getAdvancedCache().getClassLoader());
      /*
       * HACK ALERT. Initialize some internal Netty structures early while we are within the scope of the correct classloader to
       * avoid triggering ISPN-7602
       * This needs to be fixed properly within the context of ISPN-7601
       */
      new HttpResponseHandler();
   }

   @Override
   public void stop() {
      workerGroup.shutdownGracefully();
   }

   public void setInternalCacheEntryFactory(InternalEntryFactory iceFactory) {
      if (this.iceFactory != null) {
         throw new IllegalStateException();
      }
      this.iceFactory = iceFactory;
   }

   private String keyToUri(Object key) {
      try {
         return path + urlCodec.encode(key2StringMapper.getStringMapping(key));
      } catch (EncoderException e) {
         throw new PersistenceException(e);
      }
   }

   private byte[] marshall(String contentType, MarshalledEntry entry) throws IOException, InterruptedException {
      if (configuration.rawValues()) {
         return (byte[]) entry.getValue();
      } else {
         if (isTextContentType(contentType)) {
            return (byte[]) entry.getValue();
         }
         return ctx.getMarshaller().objectToByteBuffer(entry.getValue());
      }
   }

   private Object unmarshall(String contentType, byte[] b) throws IOException, ClassNotFoundException {
      if (configuration.rawValues()) {
         return b;
      } else {
         if (isTextContentType(contentType)) {
            return new String(b); // TODO: use response header Content Encoding
         } else {
            return ctx.getMarshaller().objectFromByteBuffer(b);
         }
      }
   }

   private boolean isTextContentType(String contentType) {
      if (contentType == null) return false;
      return contentType.startsWith("text/") || "application/xml".equals(contentType) || "application/json".equals(contentType);
   }

   @Override
   public void write(MarshalledEntry entry) {
      try {
         String contentType = metadataHelper.getContentType(entry);
         ByteBuf content = Unpooled.wrappedBuffer(marshall(contentType, entry));

         DefaultFullHttpRequest put = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, keyToUri(entry.getKey()), content);
         put.headers().add("Content-Type", contentType);
         put.headers().add("Content-Length", content.readableBytes());
         InternalMetadata metadata = entry.getMetadata();
         if (metadata != null && metadata.expiryTime() > -1) {
            put.headers().add(TIME_TO_LIVE_SECONDS, Long.toString(timeoutToSeconds(metadata.lifespan())));
            put.headers().add(MAX_IDLE_TIME_SECONDS, Long.toString(timeoutToSeconds(metadata.maxIdle())));
         }

         Channel ch = bootstrap.connect(configuration.host(), configuration.port()).awaitUninterruptibly().channel().pipeline().addLast(new HttpResponseHandler()).channel();
         ch.writeAndFlush(put).sync().channel().closeFuture().sync();

      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   private static class HttpResponseHandler extends SimpleChannelInboundHandler<HttpResponse> {

      private FullHttpResponse response;

      private boolean retainResponse;

      public HttpResponseHandler() {
         this(false);
      }

      public HttpResponseHandler(boolean retainResponse) {
         this.retainResponse = retainResponse;
      }

      protected void channelRead0(ChannelHandlerContext ctx, HttpResponse msg) throws Exception {
         if (retainResponse) {
            this.response = ((FullHttpResponse) msg).retain();
         }
         ctx.close();
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
         throw new PersistenceException(cause);
      }

      public FullHttpResponse getResponse() {
         return response;
      }

   }

   @Override
   public void clear() {
      DefaultHttpRequest delete = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, path);
      try {
         Channel ch = bootstrap.connect(configuration.host(), configuration.port()).awaitUninterruptibly().channel().pipeline().addLast(new HttpResponseHandler()).channel();
         ch.writeAndFlush(delete).sync().channel().closeFuture().sync();
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public boolean delete(Object key) {
      DefaultHttpRequest delete = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, keyToUri(key));
      try {
         HttpResponseHandler handler = new HttpResponseHandler(true);
         Channel ch = bootstrap.connect(configuration.host(), configuration.port()).awaitUninterruptibly().channel().pipeline().addLast(new HttpObjectAggregator(maxContentLength), handler).channel();
         ch.writeAndFlush(delete).sync().channel().closeFuture().sync();
         try {
            return isSuccessful(handler.getResponse().status().code());
         } finally {
            handler.getResponse().release();
         }

      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public MarshalledEntry load(Object key) {

      try {
         DefaultHttpHeaders headers = new DefaultHttpHeaders();
         DefaultHttpRequest get = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, keyToUri(key), headers);

         HttpResponseHandler handler = new HttpResponseHandler(true);
         Channel ch = bootstrap.connect(configuration.host(), configuration.port()).awaitUninterruptibly().channel().pipeline().addLast(new HttpObjectAggregator(maxContentLength), handler).channel();
         ch.writeAndFlush(get).sync().channel().closeFuture().sync();
         FullHttpResponse response = handler.getResponse();
         try {
            if (HttpResponseStatus.OK.equals(response.status())) {
               String contentType = response.headers().get(HttpHeaderNames.CONTENT_TYPE);
               long ttl = timeHeaderToSeconds(response.headers().get(TIME_TO_LIVE_SECONDS));
               long maxidle = timeHeaderToSeconds(response.headers().get(MAX_IDLE_TIME_SECONDS));
               Metadata metadata = metadataHelper.buildMetadata(contentType, ttl, TimeUnit.SECONDS, maxidle, TimeUnit.SECONDS);
               InternalMetadata internalMetadata;
               if (metadata.maxIdle() > -1 || metadata.lifespan() > -1) {
                  long now = ctx.getTimeService().wallClockTime();
                  internalMetadata = new InternalMetadataImpl(metadata, now, now);
               } else {
                  internalMetadata = new InternalMetadataImpl(metadata, -1, -1);
               }
               ByteBuf content = response.content();
               byte[] bytes = new byte[content.readableBytes()];
               content.readBytes(bytes);
               return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, unmarshall(contentType, bytes), internalMetadata);
            } else if (HttpResponseStatus.NOT_FOUND.equals(response.status())) {
               return null;
            } else {
               throw log.httpError(response.status().toString());
            }
         } finally {
            response.release();
         }
      } catch (IOException e) {
         throw log.httpError(e);
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   private long timeoutToSeconds(long timeout) {
      if (timeout < 0)
         return -1;
      else if (timeout > 0 && timeout < 1000)
         return 1;
      else
         return TimeUnit.MILLISECONDS.toSeconds(timeout);
   }

   private long timeHeaderToSeconds(String header) {
      return header == null ? -1 : Long.parseLong(header);
   }


   @Override
   public void process(KeyFilter keyFilter, final CacheLoaderTask cacheLoaderTask, Executor executor, boolean loadValue, boolean loadMetadata) {
      DefaultHttpRequest get = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path + "?global");
      get.headers().add(HttpHeaderNames.ACCEPT, "text/plain");
      get.headers().add(HttpHeaderNames.ACCEPT_CHARSET, "UTF-8");
      try {
         HttpResponseHandler handler = new HttpResponseHandler(true);
         Channel ch = bootstrap.connect(configuration.host(), configuration.port()).awaitUninterruptibly().channel().pipeline().addLast(
               new HttpObjectAggregator(maxContentLength), handler).channel();
         ch.writeAndFlush(get).sync().channel().closeFuture().sync();
         int batchSize = 1000;
         ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
         final TaskContext taskContext = new TaskContextImpl();
         try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteBufInputStream(((FullHttpResponse) handler.getResponse()).content()), "UTF-8"));
            try {
               Set<Object> entries = new HashSet<Object>(batchSize);
               for (String stringKey = reader.readLine(); stringKey != null; stringKey = reader.readLine()) {
                  Object key = key2StringMapper.getKeyMapping(stringKey);
                  if (keyFilter == null || keyFilter.accept(key))
                     entries.add(key);
                  if (entries.size() == batchSize) {
                     final Set<Object> batch = entries;
                     entries = new HashSet<Object>(batchSize);
                     submitProcessTask(cacheLoaderTask, eacs, taskContext, batch, loadValue, loadMetadata);
                  }
               }
               if (!entries.isEmpty()) {
                  submitProcessTask(cacheLoaderTask, eacs, taskContext, entries, loadValue, loadMetadata);
               }
            } finally {
               reader.close();
            }
            eacs.waitUntilAllCompleted();
            if (eacs.isExceptionThrown()) {
               throw new PersistenceException("Execution exception!", eacs.getFirstException());
            }
         } finally {
            handler.getResponse().release();
         }
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      }
   }

   private void submitProcessTask(final CacheLoaderTask cacheLoaderTask, CompletionService ecs,
                                  final TaskContext taskContext, final Set<Object> batch, final boolean loadEntry,
                                  final boolean loadMetadata) {
      ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               for (Object key : batch) {
                  if (taskContext.isStopped())
                     break;
                  MarshalledEntry entry = null;
                  if (loadEntry || loadMetadata) {
                     entry = load(key);
                  }
                  if (!loadEntry || !loadMetadata) {
                     entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key,
                           loadEntry ? entry.getValue() : null,
                           loadMetadata ? entry.getMetadata() : null);
                  }
                  cacheLoaderTask.processEntry(entry, taskContext);
               }
            } catch (Exception e) {
               log.errorExecutingParallelStoreTask(e);
               throw e;
            }
            return null;
         }
      });
   }

   @Override
   public void purge(Executor executor, PurgeListener purgeListener) {
      // This should be handled by the remote server
   }

   @Override
   public int size() {
      Channel ch = null;
      DefaultHttpRequest get = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path + "?global");
      get.headers().add(HttpHeaders.Names.ACCEPT, "text/plain");

      try {
         HttpResponseHandler handler = new HttpResponseHandler(true);
         ch = bootstrap.connect(configuration.host(), configuration.port()).awaitUninterruptibly().channel().pipeline().addLast(
               new HttpObjectAggregator(maxContentLength), handler).channel();
         ch.writeAndFlush(get).sync().channel().closeFuture().sync();
         try {
            BufferedReader reader = null;
            try {
               reader = new BufferedReader(new InputStreamReader(new ByteBufInputStream(((FullHttpResponse) handler.getResponse()).content())));
               int count = 0;
               while (reader.readLine() != null)
                  count++;
               return count;
            } finally {
               reader.close();
            }
         } finally {
            handler.getResponse().release();
         }
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      }
   }

   @Override
   public boolean contains(Object o) {
      return load(o) != null;
   }

   private boolean isSuccessful(int status) {
      return status >= 200 && status < 300;
   }

}
