package org.infinispan.persistence.rest;

import net.jcip.annotations.ThreadSafe;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.util.Util;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.rest.configuration.RestStoreConfiguration;
import org.infinispan.persistence.rest.logging.Log;
import org.infinispan.persistence.rest.metadata.MetadataHelper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * RestStore.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@ThreadSafe
@ConfiguredBy(RestStoreConfiguration.class)
public class RestStore implements AdvancedLoadWriteStore {
   private static final String MAX_IDLE_TIME_SECONDS = "maxIdleTimeSeconds";
   private static final String TIME_TO_LIVE_SECONDS = "timeToLiveSeconds";
   private static final Log log = LogFactory.getLog(RestStore.class, Log.class);
   private static final DateFormat RFC1123_DATEFORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
   private volatile RestStoreConfiguration configuration;
   private HttpClient httpClient;
   private InternalEntryFactory iceFactory;
   private MarshallingTwoWayKey2StringMapper key2StringMapper;
   private PoolingClientConnectionManager connectionManager;
   private String path;
   private MetadataHelper metadataHelper;
   private final URLCodec urlCodec = new URLCodec();
   private InitializationContext ctx;
   private HttpHost httpHost;


   @Override
   public void init(InitializationContext initializationContext) {
      configuration = initializationContext.getConfiguration();
      ctx = initializationContext;
   }

   @Override
   public void start()   {
      if (iceFactory == null) {
         iceFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }
      connectionManager = new PoolingClientConnectionManager();

      ConnectionPoolConfiguration pool = configuration.connectionPool();
      connectionManager.setDefaultMaxPerRoute(pool.maxConnectionsPerHost());
      connectionManager.setMaxTotal(pool.maxTotalConnections());

      HttpParams params = new BasicHttpParams();
      params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, pool.connectionTimeout());
      params.setParameter(CoreConnectionPNames.SO_TIMEOUT,  pool.socketTimeout());
      params.setParameter(CoreConnectionPNames.TCP_NODELAY, pool.tcpNoDelay());
      params.setParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, pool.bufferSize());

      httpClient = new DefaultHttpClient(connectionManager, params);

      httpHost = new HttpHost(configuration.host(), configuration.port());

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
   }

   @Override
   public void stop()   {
      connectionManager.shutdown();
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
      return contentType.startsWith("text/") || "application/xml".equals(contentType) || "application/json".equals(contentType);
   }

   @Override
   public void write(MarshalledEntry entry) {
      HttpPut put = new HttpPut(keyToUri(entry.getKey()));

      InternalMetadata metadata = entry.getMetadata();
      if (metadata != null && metadata.expiryTime() > -1) {
         put.addHeader(TIME_TO_LIVE_SECONDS, Long.toString(timeoutToSeconds(metadata.lifespan())));
         put.addHeader(MAX_IDLE_TIME_SECONDS, Long.toString(timeoutToSeconds(metadata.maxIdle())));
      }

      try {
         String contentType = metadataHelper.getContentType(entry);
         put.setEntity(new ByteArrayEntity(marshall(contentType, entry), ContentType.create(contentType)));
         httpClient.execute(httpHost, put);
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         put.abort();
      }
   }

   @Override
   public void clear() {
      HttpDelete del = new HttpDelete(path);
      try {
         HttpResponse response = httpClient.execute(httpHost, del);
         EntityUtils.consume(response.getEntity());
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         del.abort();
      }
   }

   @Override
   public boolean delete(Object key) {
      HttpDelete del = new HttpDelete(keyToUri(key));
      try {
         HttpResponse response = httpClient.execute(httpHost, del);
         EntityUtils.consume(response.getEntity());
         return isSuccessful(response.getStatusLine().getStatusCode());
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         del.abort();
      }
   }

   @Override
   public MarshalledEntry load(Object key) {
      HttpGet get = new HttpGet(keyToUri(key));
      try {
         HttpResponse response = httpClient.execute(httpHost, get);
         switch (response.getStatusLine().getStatusCode()) {
         case HttpStatus.SC_OK:
            String contentType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
            long ttl = timeHeaderToSeconds(response.getFirstHeader(TIME_TO_LIVE_SECONDS));
            long maxidle = timeHeaderToSeconds(response.getFirstHeader(MAX_IDLE_TIME_SECONDS));
            Metadata metadata = metadataHelper.buildMetadata(contentType, ttl, TimeUnit.SECONDS, maxidle, TimeUnit.SECONDS);
            InternalMetadata internalMetadata;
            if (metadata.maxIdle() > -1 || metadata.lifespan() > -1) {
               long now = ctx.getTimeService().wallClockTime();
               internalMetadata = new InternalMetadataImpl(metadata, now, now);
            } else {
               internalMetadata = new InternalMetadataImpl(metadata, -1, -1);
            }
            byte[] bytes = EntityUtils.toByteArray(response.getEntity());
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, unmarshall(contentType, bytes), internalMetadata);
            case HttpStatus.SC_NOT_FOUND:
            return null;
         default:
            throw log.httpError(response.getStatusLine().toString());
         }
      } catch (IOException e) {
         throw log.httpError(e);
      } catch (Exception e) {
         throw new PersistenceException(e);
      } finally {
         get.abort();
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

   private long timeHeaderToSeconds(Header header) {
      return header == null ? -1 : Long.parseLong(header.getValue());
   }


   @Override
   public void process(KeyFilter keyFilter, final CacheLoaderTask cacheLoaderTask, Executor executor, boolean loadValue, boolean loadMetadata) {
      HttpGet get = new HttpGet(path + "?global");
      get.addHeader(HttpHeaders.ACCEPT, "text/plain");
      try {
         HttpResponse response = httpClient.execute(httpHost, get);
         HttpEntity entity = response.getEntity();
         int batchSize = 1000;
         ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
         final TaskContext taskContext = new TaskContextImpl();
         BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
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
         eacs.waitUntilAllCompleted();
         if (eacs.isExceptionThrown()) {
            throw new PersistenceException("Execution exception!", eacs.getFirstException());
         }
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      } finally {
         get.releaseConnection();
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
                  if (!loadEntry && !loadMetadata) {
                     cacheLoaderTask.processEntry(ctx.getMarshalledEntryFactory().newMarshalledEntry(key, (Object) null, null), taskContext);
                  } else {
                     cacheLoaderTask.processEntry(load(key), taskContext);
                  }
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
      HttpGet get = new HttpGet(path + "?global");
      get.addHeader(HttpHeaders.ACCEPT, "text/plain");

      try {
         HttpResponse response = httpClient.execute(httpHost, get);
         HttpEntity entity = response.getEntity();
         BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
         int count = 0;
         while (reader.readLine() != null)
            count++;
         return count;
      } catch (Exception e) {
         throw log.errorLoadingRemoteEntries(e);
      } finally {
         get.releaseConnection();
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
