package org.infinispan.util;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.impl.Request;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.TestingUtil;

/**
 * Dispatcher that counts actually ongoing unicast RPCs. Its purpose is to isolate RPCs started before
 * {@link #advanceGenerationAndAwait(long, TimeUnit)} and those afterwards. It can handle staggered calls as well.
 */
public class CountingRequestRepository extends RequestRepository {
   private final AtomicLong generation = new AtomicLong();
   private Map<Long, Map<Long, Request<?>>> requests = new ConcurrentHashMap<>();

   public static CountingRequestRepository replaceDispatcher(EmbeddedCacheManager cacheManager) {
      GlobalComponentRegistry gcr = cacheManager.getGlobalComponentRegistry();
      JGroupsTransport transport = (JGroupsTransport) gcr.getComponent(Transport.class);
      RequestRepository requestRepository =
            (RequestRepository) TestingUtil.extractField(JGroupsTransport.class, transport, "requests");
      CountingRequestRepository instance = new CountingRequestRepository(requestRepository);
      TestingUtil.replaceField(instance, "requests", transport, JGroupsTransport.class);
      return instance;
   }

   private CountingRequestRepository(RequestRepository requestRepository) {
      requestRepository.forEach(this::addRequest);
   }

   @Override
   public void addRequest(Request<?> request) {
      requests.compute(generation.get(), (generation, map) -> {
         if (map == null) {
            map = new ConcurrentHashMap<>();
         }
         map.put(request.getRequestId(), request);
         return map;
      });
      super.addRequest(request);
   }

   /**
    * Wait until we get responses for all started requests.
    */
   public void advanceGenerationAndAwait(long timeout, TimeUnit timeUnit) throws Exception {
      long lastGen = generation.getAndIncrement();
      Map<Long, Request<?>> lastGenRequests = requests.getOrDefault(lastGen, Collections.emptyMap());
      long now = System.nanoTime();
      long deadline = now + timeUnit.toNanos(timeout);
      synchronized (this) {
         for (Map.Entry<Long, Request<?>> entry : lastGenRequests.entrySet()) {
            Request<?> request = entry.getValue();
            request.toCompletableFuture().get(deadline - now, TimeUnit.NANOSECONDS);
            now = System.currentTimeMillis();
         }
      }
   }

}
