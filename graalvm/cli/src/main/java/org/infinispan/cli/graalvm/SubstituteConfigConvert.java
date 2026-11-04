package org.infinispan.cli.graalvm;

import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.internal.ParsedCommand;
import org.infinispan.cli.activators.ConfigConversionAvailable;
import org.infinispan.cli.commands.Config;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

@TargetClass(ConfigConversionAvailable.class)
final class SubstituteConfigConversionAvailable {
   @Substitute
   public boolean isActivated(ParsedCommand command) {
      return false;
   }
}

@TargetClass(Config.Convert.class)
final class SubstituteConvert {
   @Substitute
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      throw new UnsupportedOperationException("The native CLI doesn't implement configuration conversion. Use the JVM version instead.");
   }
}
