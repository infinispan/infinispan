package org.infinispan.remoting.transport.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * @author Dan Berindei
 * @since 9.0
 */
public class RequestRepository {
   private static final Log log = LogFactory.getLog(RequestRepository.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<Long, Request<?>> requests;
   private final AtomicLong nextRequestId = new AtomicLong(1);

   public RequestRepository() {
      requests = new ConcurrentHashMap<>();
   }

   public long newRequestId() {
      long requestId = nextRequestId.getAndIncrement();
      // Make sure NO_REQUEST_ID is never used for a request
      if (requestId == Request.NO_REQUEST_ID) {
         requestId = nextRequestId.getAndIncrement();
      }
      return requestId;
   }

   public void addRequest(Request<?> request) {
      long requestId = request.getRequestId();
      Request existingRequest = requests.putIfAbsent(requestId, request);
      if (existingRequest != null) {
         throw new IllegalStateException("Duplicate request id " + requestId);
      }
   }

   public void addResponse(long requestId, Address sender, Response response) {
      Request<?> request = requests.get(requestId);
      if (request == null) {
         if (trace)
            log.tracef("Ignoring response for non-existent request %d from %s: %s", requestId, sender, response);
         return;
      }
      request.onResponse(sender, response);
   }

   public void removeRequest(long requestId) {
      requests.remove(requestId);
   }

   public void forEach(Consumer<Request<?>> consumer) {
      requests.forEach((id, request) -> consumer.accept(request));
   }
}
