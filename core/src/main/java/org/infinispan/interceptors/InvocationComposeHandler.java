package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * Callback interface for {@link InvocationStage#compose(InvocationComposeHandler)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@FunctionalInterface
public interface InvocationComposeHandler {
   BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand, Object rv,
         Throwable t) throws Throwable;
}
