package org.infinispan.rest.profiling;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.infinispan.rest.http2.NettyHttpClient;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.ssl.OpenSsl;
import io.netty.util.CharsetUtil;

/**
 * Benchmarking HTTP/1.1 and HTTP/2 is always comparing apples to bananas. Those protocols are totally
 * different and it doesn't really whether we will use the same or other clients.
 *
 * Unfortunately currently there is no good support for HTTP/2 with TLS/ALPN clients. The only implementation which
 * was reasonably good in testing was Netty (even though a lot of boilerplate code had to be generated). On the other
 * hand HTTP/1.1 is tested using Jetty client. This client unifies the API for both of them.
 */
public class BenchmarkHttpClient {

   private NettyHttpClient nettyHttpClient;
   private HttpClient http1Client;

   private ExecutorCompletionService executorCompletionService;
   private String address;
   private int port;
   private boolean http2;
   private boolean usesTLS = false;
   private ExecutorService executor;

   public BenchmarkHttpClient(String keystorePath, String keystorePassword, String trustStorePath, String trustStorePassword) throws Exception {
      if (!OpenSsl.isAlpnSupported()) {
         throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support");
      }
      nettyHttpClient = NettyHttpClient.newHttp2ClientWithALPN(keystorePath, keystorePassword);

      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setTrustStorePassword(trustStorePath);
      sslContextFactory.setTrustStorePassword(trustStorePassword);
      sslContextFactory.setKeyStorePath(keystorePath);
      sslContextFactory.setKeyStorePassword(keystorePassword);

      http1Client = new HttpClient(sslContextFactory);
      usesTLS = true;
   }

   public BenchmarkHttpClient() {
      nettyHttpClient = NettyHttpClient.newHttp2ClientWithHttp11Upgrade();
      http1Client = new HttpClient();
   }

   public void performGets(int pertentageOfMisses, int numberOfGets, String existingKey, String nonExistingKey) throws Exception {
      Random r = ThreadLocalRandom.current();
      for (int i = 0; i < numberOfGets; ++i) {
         String key = r.nextInt(100) < pertentageOfMisses ? nonExistingKey : existingKey;
         executorCompletionService.submit(() -> {
            if (http2) {
               FullHttpRequest getRequest = new DefaultFullHttpRequest(HTTP_1_1, GET, "/rest/default/" + key);
               nettyHttpClient.sendRequest(getRequest);
            } else {
               try {
                  String scheme = usesTLS ? "https" : "http";
                  http1Client
                        .GET(String.format("%s://localhost:%d/rest/%s/%s", scheme, port, "default", key));
               } catch (Exception e) {
                  throw new RuntimeException(e);
               }
            }
            return 1;
         });
      }
      for (int i = 0; i < numberOfGets; ++i) {
         executorCompletionService.take().get();
      }
      if (http2) {
         nettyHttpClient.getResponses();
      }
   }

   public void performPuts(int numberOfInserts) throws Exception {
      for (int i = 0; i < numberOfInserts; ++i) {
         String randomKey = UUID.randomUUID().toString();
         executorCompletionService.submit(() -> {
            if (http2) {
               FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/default/" + randomKey,
                     wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));
               nettyHttpClient.sendRequest(putValueInCacheRequest);
               return 1;
            } else {
               try {
                  String scheme = usesTLS ? "https" : "http";
                  http1Client
                        .POST(String.format("%s://localhost:%d/rest/%s/%s", scheme, port, "default", "randomKey"))
                        .content(new StringContentProvider("test"))
                        .send();
                  return 1;
               } catch (Exception e) {
                  throw new RuntimeException(e);
               }
            }
         });
      }
      for (int i = 0; i < numberOfInserts; ++i) {
         executorCompletionService.take().get();
      }
      if (http2) {
         nettyHttpClient.getResponses();
      }
   }

   public void start(String address, int port, int threads, boolean http2) throws Exception {
      this.address = address;
      this.port = port;
      this.http2 = http2;
      if (http2)
         nettyHttpClient.start(address, port);
      else
         http1Client.start();
      executor = Executors.newFixedThreadPool(threads);
      executorCompletionService = new ExecutorCompletionService(executor);
   }

   public void stop() throws Exception {
      if (http2)
         nettyHttpClient.stop();
      else
         http1Client.stop();
      executor.shutdownNow();
   }

}
