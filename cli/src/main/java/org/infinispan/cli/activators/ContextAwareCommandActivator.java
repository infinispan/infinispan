package org.infinispan.cli.activators;

import org.aesh.command.activator.CommandActivator;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface ContextAwareCommandActivator extends CommandActivator {
   void setContext(Context context);
}
