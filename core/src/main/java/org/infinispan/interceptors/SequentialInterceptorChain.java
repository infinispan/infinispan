package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.AnyInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;

import java.util.List;
import java.util.stream.Stream;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface SequentialInterceptorChain {
   List<SequentialInterceptor> getSequentialInterceptors();

   List<AnyInterceptor> getInterceptors();

   /**
    * Inserts the given interceptor at the specified position in the chain (o based indexing).
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   void addInterceptor(AnyInterceptor interceptor, int position);

   /**
    * Removes the interceptor at the given postion.
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   void removeInterceptor(int position);

   /**
    * Returns the number of interceptors in the chain.
    */
   int size();

   /**
    * Removes all the occurences of supplied interceptor type from the chain.
    */
   void removeInterceptor(Class<? extends AnyInterceptor> clazz);

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   boolean addInterceptorAfter(AnyInterceptor toAdd, Class<? extends 
         AnyInterceptor> afterInterceptor);

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   boolean addInterceptorBefore(AnyInterceptor toAdd,
                                                Class<? extends AnyInterceptor> beforeInterceptor);

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor instance passed as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    * @return true if the interceptor was replaced
    */
   boolean replaceInterceptor(AnyInterceptor replacingInterceptor,
                                              Class<? extends AnyInterceptor> toBeReplacedInterceptorType);

   /**
    * Appends at the end.
    */
   void appendInterceptor(AnyInterceptor ci, boolean isCustom);

   /**
    * Walks the command through the interceptor chain. The received ctx is being passed in.
    */
   Object invoke(InvocationContext ctx, VisitableCommand command);

   /**
    * Returns all interceptors which extend the given command interceptor.
    */
   AnyInterceptor findInterceptorExtending(Class<? extends AnyInterceptor> interceptorClass);

   /**
    * Returns all the interceptors that have the fully qualified name of their class equal with the supplied class
    * name.
    */
   AnyInterceptor findInterceptorWithClass(Class<? extends AnyInterceptor> interceptorClass);

   /**
    * Checks whether the chain contains the supplied interceptor instance.
    */
   boolean containsInstance(AnyInterceptor interceptor);

   boolean containsInterceptorType(Class<? extends AnyInterceptor> interceptorType);

   boolean containsInterceptorType(Class<? extends AnyInterceptor> interceptorType,
                                                   boolean alsoMatchSubClasses);
}
