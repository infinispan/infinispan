package org.infinispan.interceptors;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;

/**
 * Interceptor chain using {@link AsyncInterceptor}s.
 *
 * Experimental: The ability to modify the interceptors at runtime may be removed in future versions.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public interface AsyncInterceptorChain {
   /**
    * @return An immutable list of the current interceptors.
    */
   List<AsyncInterceptor> getInterceptors();

   /**
    * Inserts the given interceptor at the specified position in the chain (0 based indexing).
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors
    *       in the chain)
    */
   void addInterceptor(AsyncInterceptor interceptor, int position);

   /**
    * Removes the interceptor at the given position.
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors
    *       in the chain)
    */
   void removeInterceptor(int position);

   /**
    * Returns the number of interceptors in the chain.
    */
   int size();

   /**
    * Removes all the occurrences of supplied interceptor type from the chain.
    */
   void removeInterceptor(Class<? extends AsyncInterceptor> clazz);

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the {@code afterInterceptor} exists
    */
   boolean addInterceptorAfter(AsyncInterceptor toAdd, Class<? extends
         AsyncInterceptor> afterInterceptor);

   /**
    * Adds a new interceptor in list before an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the {@code beforeInterceptor} exists
    */
   boolean addInterceptorBefore(AsyncInterceptor toAdd, Class<? extends AsyncInterceptor> beforeInterceptor);

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor
    * instance passed as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    * @return true if the interceptor was replaced
    */
   boolean replaceInterceptor(AsyncInterceptor replacingInterceptor,
         Class<? extends AsyncInterceptor> toBeReplacedInterceptorType);

   /**
    * Appends at the end.
    */
   void appendInterceptor(AsyncInterceptor ci, boolean isCustom);

   /**
    * Walks the command through the interceptor chain. The received ctx is being passed in.
    *
    * <p>Note: Reusing the context for multiple invocations is allowed, however most context implementations are not
    * thread-safe.</p>
    */
   Object invoke(InvocationContext ctx, VisitableCommand command);

   /**
    * Walks the command through the interceptor chain. The received ctx is being passed in.
    */
   CompletableFuture<Object> invokeAsync(InvocationContext ctx, VisitableCommand command);

   /**
    * Returns the first interceptor extending the given class, or {@code null} if there is none.
    */
   <T extends AsyncInterceptor> T findInterceptorExtending(Class<T> interceptorClass);

   /**
    * Returns the first interceptor with the given class, or {@code null} if there is none.
    */
   <T extends AsyncInterceptor> T findInterceptorWithClass(Class<T> interceptorClass);

   /**
    * Checks whether the chain contains the supplied interceptor instance.
    */
   boolean containsInstance(AsyncInterceptor interceptor);

   /**
    * Checks whether the chain contains an interceptor with the given class.
    */
   boolean containsInterceptorType(Class<? extends AsyncInterceptor> interceptorType);

   /**
    * Checks whether the chain contains an interceptor with the given class, or a subclass.
    */
   boolean containsInterceptorType(Class<? extends AsyncInterceptor> interceptorType,
                                                   boolean alsoMatchSubClasses);
}
