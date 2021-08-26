package org.infinispan.interceptors;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class EmptyAsyncInterceptorChain implements AsyncInterceptorChain {
   public static final EmptyAsyncInterceptorChain INSTANCE = new EmptyAsyncInterceptorChain();

   @Override
   public List<AsyncInterceptor> getInterceptors() {
      return Collections.emptyList();
   }

   @Override
   public void addInterceptor(AsyncInterceptor interceptor, int position) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public void removeInterceptor(int position) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public void removeInterceptor(Class<? extends AsyncInterceptor> clazz) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorAfter(AsyncInterceptor toAdd,
         Class<? extends AsyncInterceptor> afterInterceptor) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorBefore(AsyncInterceptor toAdd,
         Class<? extends AsyncInterceptor> beforeInterceptor) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public boolean replaceInterceptor(AsyncInterceptor replacingInterceptor,
         Class<? extends AsyncInterceptor> toBeReplacedInterceptorType) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public void appendInterceptor(AsyncInterceptor ci, boolean isCustom) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public CompletableFuture<Object> invokeAsync(InvocationContext ctx, VisitableCommand command) {
      throw CONTAINER.interceptorStackNotSupported();
   }

   @Override
   public InvocationStage invokeStage(InvocationContext ctx, VisitableCommand command) {
      throw CONTAINER.interceptorStackNotSupported();
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
