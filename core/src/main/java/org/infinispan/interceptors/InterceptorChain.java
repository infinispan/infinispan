package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.Collections;
import java.util.List;

/**
 * Knows how to build and manage an chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Since 8.1, use {@link SequentialInterceptorChain} instead.
 */
@Scope(Scopes.NAMED_CACHE)
@Deprecated
public abstract class InterceptorChain implements SequentialInterceptorChain {
   /**
    * Inserts the given interceptor at the specified position in the chain (o based indexing).
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   public abstract void addInterceptor(CommandInterceptor interceptor, int position);

   /**
    * Removes the interceptor at the given postion.
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   public abstract void removeInterceptor(int position);

   /**
    * Returns the number of interceptors in the chain.
    */
   public abstract int size();

   /**
    * @deprecated Always returns an empty list, since not all interceptors are CommandInterceptors.
    */
   public List<CommandInterceptor> asList() {
      return Collections.emptyList();
   }

   /**
    * Removes all the occurences of supplied interceptor type from the chain.
    */
   public abstract void removeInterceptor(Class<? extends CommandInterceptor> clazz);

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   public abstract boolean addInterceptorAfter(CommandInterceptor toAdd, Class<? extends CommandInterceptor> afterInterceptor);

   /**
    * @deprecated Use {@link #addInterceptorBefore(CommandInterceptor,
    * Class)} instead.
    */
   @Deprecated
   public abstract boolean addInterceptorBefore(CommandInterceptor toAdd,
                                Class<? extends CommandInterceptor> beforeInterceptor, boolean isCustom);

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   public abstract boolean addInterceptorBefore(CommandInterceptor toAdd,
                                Class<? extends CommandInterceptor> beforeInterceptor);

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor instance passed as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    * @return true if the interceptor was replaced
    */
   public abstract boolean replaceInterceptor(CommandInterceptor replacingInterceptor,
                              Class<? extends CommandInterceptor> toBeReplacedInterceptorType);

   /**
    * Appends at the end.
    */
   public abstract void appendInterceptor(CommandInterceptor ci, boolean isCustom);

   /**
    * Walks the command through the interceptor chain. The received ctx is being passed in.
    */
   public abstract Object invoke(InvocationContext ctx, VisitableCommand command);

   /**
    * @return the first interceptor in the chain.
    */

   public abstract CommandInterceptor getFirstInChain();

   /**
    * Mainly used by unit tests to replace the interceptor chain with the starting point passed in.
    *
    * @param interceptor interceptor to be used as the first interceptor in the chain.
    */
   public abstract void setFirstInChain(CommandInterceptor interceptor);

   /**
    * Returns all interceptors which extend the given command interceptor.
    */
   public abstract List<CommandInterceptor> getInterceptorsWhichExtend(Class<? extends CommandInterceptor> interceptorClass);

   /**
    * Returns all the interceptors that have the fully qualified name of their class equal with the supplied class
    * name.
    */
   public abstract List<CommandInterceptor> getInterceptorsWithClass(Class clazz);

   /**
    * Checks whether the chain contains the supplied interceptor instance.
    */
   public abstract boolean containsInstance(CommandInterceptor interceptor);

   public abstract boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType);

   public abstract boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType,
                                   boolean alsoMatchSubClasses);

}
