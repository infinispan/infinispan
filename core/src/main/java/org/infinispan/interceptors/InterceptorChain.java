package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to build and manage an chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Since 9.0, use {@link SequentialInterceptorChain} instead. Some methods will ignore the
 * interceptors that do not extend {@link CommandInterceptor}.
 */
@Scope(Scopes.NAMED_CACHE)
@Deprecated
public class InterceptorChain {
   private SequentialInterceptorChain sequentialInterceptorChain;

   public InterceptorChain(SequentialInterceptorChain sequentialInterceptorChain) {
      this.sequentialInterceptorChain = sequentialInterceptorChain;
   }

   /**
    * Inserts the given interceptor at the specified position in the chain (o based indexing).
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   public void addInterceptor(CommandInterceptor interceptor, int position) {
      sequentialInterceptorChain.addInterceptor(interceptor, position);
   }

   /**
    * Removes the interceptor at the given postion.
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   public void removeInterceptor(int position) {
      sequentialInterceptorChain.removeInterceptor(position);
   }

   /**
    * Returns the number of interceptors in the chain.
    */
   public int size() {
      return sequentialInterceptorChain.size();
   }

   /**
    * Returns an unmofiable list with all the interceptors in sequence. If first in chain is null an empty list is
    * returned.
    */
   public List<CommandInterceptor> asList() {
      ArrayList<CommandInterceptor> list =
            new ArrayList<>(sequentialInterceptorChain.getInterceptors().size());
      sequentialInterceptorChain.getInterceptors().forEach(ci -> {
         if (ci instanceof CommandInterceptor) {
            list.add((CommandInterceptor) ci);
         }
      });
      return list;
   }

   /**
    * Removes all the occurences of supplied interceptor type from the chain.
    */
   public void removeInterceptor(Class<? extends CommandInterceptor> clazz) {
      sequentialInterceptorChain.removeInterceptor(clazz);
   }

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   public boolean addInterceptorAfter(CommandInterceptor toAdd,
                                      Class<? extends CommandInterceptor> afterInterceptor) {
      return sequentialInterceptorChain.addInterceptorAfter(toAdd, afterInterceptor);
   }

   /**
    * @deprecated Use {@link #addInterceptorBefore(CommandInterceptor, Class)} instead.
    */
   @Deprecated
   public boolean addInterceptorBefore(CommandInterceptor toAdd,
                                       Class<? extends CommandInterceptor> beforeInterceptor,
                                       boolean isCustom) {
      return sequentialInterceptorChain.addInterceptorBefore(toAdd, beforeInterceptor);
   }

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   public boolean addInterceptorBefore(CommandInterceptor toAdd,
                                       Class<? extends CommandInterceptor> beforeInterceptor) {
      return sequentialInterceptorChain.addInterceptorBefore(toAdd, beforeInterceptor);
   }

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor instance passed as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    * @return true if the interceptor was replaced
    */
   public boolean replaceInterceptor(CommandInterceptor replacingInterceptor,
                                     Class<? extends CommandInterceptor> toBeReplacedInterceptorType) {
      return sequentialInterceptorChain.replaceInterceptor(replacingInterceptor, toBeReplacedInterceptorType);
   }

   /**
    * Appends at the end.
    */
   public void appendInterceptor(CommandInterceptor ci, boolean isCustom) {
      sequentialInterceptorChain.appendInterceptor(ci, isCustom);
   }

   /**
    * Walks the command through the interceptor chain. The received ctx is being passed in.
    *
    * <p>Note: Reusing the context for multiple invocations is allowed. However, the two invocations
    * must not overlap, so calling {@link #invoke(InvocationContext, VisitableCommand)} from an interceptor
    * is not allowed. If an interceptor wants to invoke a new command, it must first copy the invocation
    * context with {@link InvocationContext#clone()}.</p>
    */
   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      return sequentialInterceptorChain.invoke(ctx, command);
   }

   /**
    * @return the first {@code CommandInterceptor} in the chain.
    * Since 9.0, there will likely be other {@link SequentialInterceptor}s before it.
    */
   @Deprecated
   public CommandInterceptor getFirstInChain() {
      return (CommandInterceptor) sequentialInterceptorChain
            .findInterceptorExtending(CommandInterceptor.class);
   }

   /**
    * Mainly used by unit tests to replace the interceptor chain with the starting point passed in.
    *
    * @param interceptor interceptor to be used as the first interceptor in the chain.
    */
   public void setFirstInChain(CommandInterceptor interceptor) {
      addInterceptor(interceptor, 0);
   }

   /**
    * Returns all interceptors which extend the given command interceptor.
    */
   public List<CommandInterceptor> getInterceptorsWhichExtend(
         Class<? extends CommandInterceptor> interceptorClass) {
      ArrayList<CommandInterceptor> list =
            new ArrayList<>(sequentialInterceptorChain.getInterceptors().size());
      sequentialInterceptorChain.getInterceptors().forEach(ci -> {
         if (interceptorClass.isInstance(ci)) {
            list.add((CommandInterceptor) ci);
         }
      });
      return list;
   }

   /**
    * Returns all the interceptors that have the fully qualified name of their class equal with the supplied class
    * name.
    */
   public List<CommandInterceptor> getInterceptorsWithClass(Class clazz) {
      ArrayList<CommandInterceptor> list =
            new ArrayList<>(sequentialInterceptorChain.getInterceptors().size());
      sequentialInterceptorChain.getInterceptors().forEach(ci -> {
         if (clazz == ci.getClass()) {
            list.add((CommandInterceptor) ci);
         }
      });
      return list;
   }

   /**
    * Checks whether the chain contains the supplied interceptor instance.
    */
   public boolean containsInstance(CommandInterceptor interceptor) {
      return sequentialInterceptorChain.containsInstance(interceptor);
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType) {
      return sequentialInterceptorChain.containsInterceptorType(interceptorType);
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType,
                                          boolean alsoMatchSubClasses) {
      return sequentialInterceptorChain.containsInterceptorType(interceptorType, alsoMatchSubClasses);
   }
}
