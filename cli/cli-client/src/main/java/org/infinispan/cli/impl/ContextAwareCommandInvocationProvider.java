package org.infinispan.cli.impl;

import java.util.Objects;

import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.invocation.CommandInvocationProvider;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareCommandInvocationProvider implements CommandInvocationProvider {
   private final Context context;

   public ContextAwareCommandInvocationProvider(Context context) {
      Objects.nonNull(context);
      this.context = context;
   }

   @Override
   public CommandInvocation enhanceCommandInvocation(CommandInvocation commandInvocation) {
      return new ContextAwareCommandInvocation(commandInvocation, context);
   }
}
