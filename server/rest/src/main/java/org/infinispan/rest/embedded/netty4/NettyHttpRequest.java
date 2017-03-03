package org.infinispan.rest.embedded.netty4;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.infinispan.rest.embedded.netty4.i18n.Messages;
import org.jboss.resteasy.core.AbstractAsynchronousResponse;
import org.jboss.resteasy.core.AbstractExecutionContext;
import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.plugins.server.BaseHttpRequest;
import org.jboss.resteasy.specimpl.ResteasyHttpHeaders;
import org.jboss.resteasy.spi.NotImplementedYetException;
import org.jboss.resteasy.spi.ResteasyAsynchronousContext;
import org.jboss.resteasy.spi.ResteasyAsynchronousResponse;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.jboss.resteasy.spi.UnhandledException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;

/**
 * Abstraction for an inbound http request on the server, or a response from a server to a client
 * <p/>
 * We have this abstraction so that we can reuse marshalling objects in a client framework and serverside framework
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @author Norman Maurer
 * @author Kristoffer Sjogren
 * @version $Revision: 1 $
 * Temporary fork from RestEasy 3.1.0
 */
public class NettyHttpRequest extends BaseHttpRequest {
   private final boolean is100ContinueExpected;
   private final ChannelHandlerContext ctx;
   protected ResteasyHttpHeaders httpHeaders;
   protected SynchronousDispatcher dispatcher;
   protected String httpMethod;
   protected InputStream inputStream;
   protected Map<String, Object> attributes = new HashMap<String, Object>();
   protected NettyHttpResponse response;
   private NettyExecutionContext executionContext;
   private volatile boolean flushed;
   private ByteBuf content;

   public NettyHttpRequest(ChannelHandlerContext ctx, ResteasyHttpHeaders httpHeaders, ResteasyUriInfo uri, String httpMethod, SynchronousDispatcher dispatcher, NettyHttpResponse response, boolean is100ContinueExpected) {
      super(uri);
      this.is100ContinueExpected = is100ContinueExpected;
      this.response = response;
      this.dispatcher = dispatcher;
      this.httpHeaders = httpHeaders;
      this.httpMethod = httpMethod;
      this.executionContext = new NettyExecutionContext(this, response, dispatcher);
      this.ctx = ctx;
   }

   @Override
   public MultivaluedMap<String, String> getMutableHeaders() {
      return httpHeaders.getMutableHeaders();
   }

   @Override
   public Enumeration<String> getAttributeNames() {
      Enumeration<String> en = new Enumeration<String>() {
         private Iterator<String> it = attributes.keySet().iterator();

         @Override
         public boolean hasMoreElements() {
            return it.hasNext();
         }

         @Override
         public String nextElement() {
            return it.next();
         }
      };
      return en;
   }

   @Override
   public ResteasyAsynchronousContext getAsyncContext() {
      return executionContext;
   }

   public boolean isFlushed() {
      return flushed;
   }

   @Override
   public Object getAttribute(String attribute) {
      return attributes.get(attribute);
   }

   @Override
   public void setAttribute(String name, Object value) {
      attributes.put(name, value);
   }

   @Override
   public void removeAttribute(String name) {
      attributes.remove(name);
   }

   @Override
   public HttpHeaders getHttpHeaders() {
      return httpHeaders;
   }

   @Override
   public InputStream getInputStream() {
      return inputStream;
   }

   @Override
   public void setInputStream(InputStream stream) {
      this.inputStream = stream;
   }

   @Override
   public String getHttpMethod() {
      return httpMethod;
   }

   @Override
   public void setHttpMethod(String method) {
      this.httpMethod = method;
   }

   public NettyHttpResponse getResponse() {
      return response;
   }

   public boolean isKeepAlive() {
      return response.isKeepAlive();
   }

   public boolean is100ContinueExpected() {
      return is100ContinueExpected;
   }

   @Override
   public void forward(String path) {
      throw new NotImplementedYetException();
   }

   @Override
   public boolean wasForwarded() {
      return false;
   }

   public void setContentBuffer(ByteBuf content) {
      this.content = content;
      this.inputStream = new ByteBufInputStream(content);
   }

   public void releaseContentBuffer() {
      if (content != null) {
         this.content.release();
      }
   }

   @Override
   public String toString() {
      return "NettyHttpRequest{" +
            "httpMethod='" + httpMethod + '\'' +
            ", httpHeaders=" + httpHeaders +
            '}';
   }

   class NettyExecutionContext extends AbstractExecutionContext {
      protected final NettyHttpRequest request;
      protected final NettyHttpResponse response;
      protected volatile boolean done;
      protected volatile boolean cancelled;
      protected volatile boolean wasSuspended;
      protected NettyHttpAsyncResponse asyncResponse;

      public NettyExecutionContext(NettyHttpRequest request, NettyHttpResponse response, SynchronousDispatcher dispatcher) {
         super(dispatcher, request, response);
         this.request = request;
         this.response = response;
         this.asyncResponse = new NettyHttpAsyncResponse(dispatcher, request, response);
      }

      @Override
      public boolean isSuspended() {
         return wasSuspended;
      }

      @Override
      public ResteasyAsynchronousResponse getAsyncResponse() {
         return asyncResponse;
      }

      @Override
      public ResteasyAsynchronousResponse suspend() throws IllegalStateException {
         return suspend(-1);
      }

      @Override
      public ResteasyAsynchronousResponse suspend(long millis) throws IllegalStateException {
         return suspend(millis, TimeUnit.MILLISECONDS);
      }

      @Override
      public ResteasyAsynchronousResponse suspend(long time, TimeUnit unit) throws IllegalStateException {
         if (wasSuspended) {
            throw new IllegalStateException(Messages.MESSAGES.alreadySuspended());
         }
         wasSuspended = true;
         return asyncResponse;
      }


      /**
       * Netty implementation of {@link AsyncResponse}.
       *
       * @author Kristoffer Sjogren
       */
      class NettyHttpAsyncResponse extends AbstractAsynchronousResponse {
         private final Object responseLock = new Object();
         protected ScheduledFuture timeoutFuture;
         private NettyHttpResponse nettyResponse;

         public NettyHttpAsyncResponse(SynchronousDispatcher dispatcher, NettyHttpRequest request, NettyHttpResponse response) {
            super(dispatcher, request, response);
            this.nettyResponse = response;
         }

         @Override
         public void initialRequestThreadFinished() {
            // done
         }

         @Override
         public boolean resume(Object entity) {
            synchronized (responseLock) {
               if (done) return false;
               if (cancelled) return false;
               try {
                  return internalResume(entity);
               } finally {
                  done = true;
                  nettyFlush();
               }
            }
         }

         @Override
         public boolean resume(Throwable ex) {
            synchronized (responseLock) {
               if (done) return false;
               if (cancelled) return false;
               try {
                  return internalResume(ex);
               } catch (UnhandledException unhandled) {
                  return internalResume(Response.status(500).build());
               } finally {
                  done = true;
                  nettyFlush();
               }
            }
         }

         @Override
         public boolean cancel() {
            synchronized (responseLock) {
               if (cancelled) {
                  return true;
               }
               if (done) {
                  return false;
               }
               done = true;
               cancelled = true;
               try {
                  return internalResume(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
               } finally {
                  nettyFlush();
               }
            }
         }

         @Override
         public boolean cancel(int retryAfter) {
            synchronized (responseLock) {
               if (cancelled) return true;
               if (done) return false;
               done = true;
               cancelled = true;
               try {
                  return internalResume(Response.status(Response.Status.SERVICE_UNAVAILABLE).header(HttpHeaders.RETRY_AFTER, retryAfter).build());
               } finally {
                  nettyFlush();
               }
            }
         }

         protected synchronized void nettyFlush() {
            flushed = true;
            try {
               nettyResponse.finish();
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }

         @Override
         public boolean cancel(Date retryAfter) {
            synchronized (responseLock) {
               if (cancelled) return true;
               if (done) return false;
               done = true;
               cancelled = true;
               try {
                  return internalResume(Response.status(Response.Status.SERVICE_UNAVAILABLE).header(HttpHeaders.RETRY_AFTER, retryAfter).build());
               } finally {
                  nettyFlush();
               }
            }
         }

         @Override
         public boolean isSuspended() {
            return !done && !cancelled;
         }

         @Override
         public boolean isCancelled() {
            return cancelled;
         }

         @Override
         public boolean isDone() {
            return done;
         }

         @Override
         public boolean setTimeout(long time, TimeUnit unit) {
            synchronized (responseLock) {
               if (done || cancelled) return false;
               if (timeoutFuture != null && !timeoutFuture.cancel(false)) {
                  return false;
               }
               Runnable task = new Runnable() {
                  @Override
                  public void run() {
                     handleTimeout();
                  }
               };
               timeoutFuture = ctx.executor().schedule(task, time, unit);
            }
            return true;
         }

         protected void handleTimeout() {
            if (timeoutHandler != null) {
               timeoutHandler.handleTimeout(this);
            }
            if (done) return;
            resume(new ServiceUnavailableException());
         }
      }
   }
}
