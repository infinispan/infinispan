package org.infinispan.cli.activators;

import org.aesh.command.activator.CommandActivator;
import org.aesh.command.impl.internal.ParsedCommand;

/**
 * An activator which is used for marking commands which are not yet implemented
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class DisabledActivator implements CommandActivator {
   @Override
   public boolean isActivated(ParsedCommand command) {
      return false;
   }
}
