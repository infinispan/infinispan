package org.infinispan.interceptors;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class EmptyAsyncInterceptorChain implements AsyncInterceptorChain {
   public static final EmptyAsyncInterceptorChain INSTANCE = new EmptyAsyncInterceptorChain();

   private static final Log log = LogFactory.getLog(EmptyAsyncInterceptorChain.class);

   @Override
   public List<AsyncInterceptor> getInterceptors() {
      return Collections.emptyList();
   }

   @Override
   public void addInterceptor(AsyncInterceptor interceptor, int position) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public void removeInterceptor(int position) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public void removeInterceptor(Class<? extends AsyncInterceptor> clazz) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorAfter(AsyncInterceptor toAdd,
         Class<? extends AsyncInterceptor> afterInterceptor) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorBefore(AsyncInterceptor toAdd,
         Class<? extends AsyncInterceptor> beforeInterceptor) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean replaceInterceptor(AsyncInterceptor replacingInterceptor,
         Class<? extends AsyncInterceptor> toBeReplacedInterceptorType) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public void appendInterceptor(AsyncInterceptor ci, boolean isCustom) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public CompletableFuture<Object> invokeAsync(InvocationContext ctx, VisitableCommand command) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public <T extends AsyncInterceptor> T findInterceptorExtending(Class<T> interceptorClass) {
      return null;
   }

   @Override
   public <T extends AsyncInterceptor> T findInterceptorWithClass(Class<T> interceptorClass) {
      return null;
   }

   @Override
   public boolean containsInstance(AsyncInterceptor interceptor) {
      return false;
   }

   @Override
   public boolean containsInterceptorType(Class<? extends AsyncInterceptor> interceptorType) {
      return false;
   }

   @Override
   public boolean containsInterceptorType(Class<? extends AsyncInterceptor> interceptorType,
         boolean alsoMatchSubClasses) {
      return false;
   }
}
