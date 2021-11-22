package org.infinispan.cli.activators;

import org.aesh.command.activator.CommandActivator;
import org.aesh.command.impl.internal.ParsedCommand;
import org.infinispan.commons.util.Util;

/**
 * @since 14.0
 **/
public class ConfigConversionAvailable implements CommandActivator {
   @Override
   public boolean isActivated(ParsedCommand command) {
      try {
         Util.loadClass("org.infinispan.configuration.parsing.ParserRegistry", this.getClass().getClassLoader());
         return true;
      } catch (Exception e) {
         return false;
      }
   }
}
