package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;

import org.aesh.command.completer.CompleterInvocation;
import org.aesh.command.completer.OptionCompleter;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class ListCompleter implements OptionCompleter<ContextAwareCompleterInvocation> {

   abstract Collection<String> getAvailableItems(Context context) throws IOException;

   protected Collection<String> getAvailableItems(ContextAwareCompleterInvocation invocation) throws IOException {
      return getAvailableItems(invocation.context);
   }

   @Override
   public void complete(ContextAwareCompleterInvocation invocation) {
      try {
         Collection<String> all = getAvailableItems(invocation);
         completeFromList(invocation, all);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   public void completeFromList(CompleterInvocation invocation, Collection<String> all) {
      String v = invocation.getGivenCompleteValue();
      if (v == null || v.length() == 0) {
         invocation.addAllCompleterValues(all);
      } else {
         for (String item : all) {
            if (item.startsWith(v)) {
               invocation.addCompleterValue(item);
            }
         }
      }
   }
}
