package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.aesh.command.completer.CompleterInvocation;
import org.aesh.command.completer.OptionCompleter;
import org.infinispan.cli.Context;
import org.infinispan.cli.resources.Resource;
import org.infinispan.cli.resources.RootResource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CdContextCompleter implements OptionCompleter<CompleterInvocation> {

   @Override
   public void complete(CompleterInvocation invocation) {
      Context context = ((ContextAwareCompleterInvocation) invocation).context;
      Resource resource = context.getConnection().getActiveResource();
      String v = invocation.getGivenCompleteValue();
      if (v == null || v.length() == 0) {
         // no completions yet, add all of the local resource children
         invocation.addAllCompleterValues(getChildrenNames(resource));
         invocation.setAppendSpace(resource.isLeaf());
      } else {
         String[] parts = v.split("/");
         if (parts.length == 0) {
            resource = resource.findAncestor(RootResource.class);
            invocation.addAllCompleterValues(getChildrenNames(resource));
            invocation.setAppendSpace(resource.isLeaf());
         } else {
            int offset;
            String last;
            String prefix;
            if (v.endsWith("/")) {
               offset = 0;
               last = "";
               prefix = v;
            } else {
               offset = 1;
               last = parts[parts.length - 1];
               int lastSlash = v.lastIndexOf('/');
               prefix = lastSlash < 0 ? "" : v.substring(0, lastSlash + 1);
            }
            for (int i = 0; i < parts.length - offset; i++) {
               if (parts[i].isEmpty()) {
                  resource = resource.findAncestor(RootResource.class);
               } else {
                  try {
                     resource = resource.getChild(parts[i]);
                  } catch (IOException e) {
                     // Ignore
                  }
               }
            }
            Iterable<String> all = getChildrenNames(resource);
            for (String item : all) {
               if (item.startsWith(last)) {
                  invocation.addCompleterValue(prefix + item);
               }
            }
            invocation.setAppendSpace(resource.isLeaf());
         }
      }
   }

   private Collection<String> getChildrenNames(Resource resource) {
      try {
         List<String> children = new ArrayList<>();
         resource.getChildrenNames().forEach(children::add);
         return children;
      } catch (IOException e) {
         return Collections.emptyList();
      }
   }
}
