package org.infinispan.cli.activators;

import org.aesh.command.activator.CommandActivator;
import org.aesh.command.impl.internal.ParsedCommand;
import org.infinispan.commons.util.Features;

/**
 * @since 13.0
 **/
public class InstallFeatureActivator implements CommandActivator {
   @Override
   public boolean isActivated(ParsedCommand command) {
      return new Features().isAvailable("install");
   }
}
