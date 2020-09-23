package org.infinispan.cli.completers;

import java.util.Objects;

import org.aesh.command.completer.CompleterInvocation;
import org.aesh.command.completer.CompleterInvocationProvider;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareCompleterInvocationProvider implements CompleterInvocationProvider {
   private final Context context;

   public ContextAwareCompleterInvocationProvider(Context context) {
      Objects.nonNull(context);
      this.context = context;
   }

   @Override
   public CompleterInvocation enhanceCompleterInvocation(CompleterInvocation completerInvocation) {
      return new ContextAwareCompleterInvocation(completerInvocation, context);
   }
}
