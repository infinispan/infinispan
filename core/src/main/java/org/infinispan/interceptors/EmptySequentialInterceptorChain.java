package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class EmptySequentialInterceptorChain implements SequentialInterceptorChain {
   public static final EmptySequentialInterceptorChain INSTANCE = new EmptySequentialInterceptorChain();

   private static final Log log = LogFactory.getLog(EmptySequentialInterceptorChain.class);

   @Override
   public List<SequentialInterceptor> getInterceptors() {
      return Collections.emptyList();
   }

   @Override
   public void addInterceptor(SequentialInterceptor interceptor, int position) {
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
   public void removeInterceptor(Class<? extends SequentialInterceptor> clazz) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorAfter(SequentialInterceptor toAdd,
         Class<? extends SequentialInterceptor> afterInterceptor) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorBefore(SequentialInterceptor toAdd,
         Class<? extends SequentialInterceptor> beforeInterceptor) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean replaceInterceptor(SequentialInterceptor replacingInterceptor,
         Class<? extends SequentialInterceptor> toBeReplacedInterceptorType) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public void appendInterceptor(SequentialInterceptor ci, boolean isCustom) {
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
   public <T extends SequentialInterceptor> T findInterceptorExtending(Class<T> interceptorClass) {
      return null;
   }

   @Override
   public <T extends SequentialInterceptor> T findInterceptorWithClass(Class<T> interceptorClass) {
      return null;
   }

   @Override
   public boolean containsInstance(SequentialInterceptor interceptor) {
      return false;
   }

   @Override
   public boolean containsInterceptorType(Class<? extends SequentialInterceptor> interceptorType) {
      return false;
   }

   @Override
   public boolean containsInterceptorType(Class<? extends SequentialInterceptor> interceptorType,
         boolean alsoMatchSubClasses) {
      return false;
   }
}
