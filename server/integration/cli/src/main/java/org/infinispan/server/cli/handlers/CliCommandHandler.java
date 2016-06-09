package org.infinispan.server.cli.handlers;

import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandFormatException;
import org.jboss.as.cli.CommandLineException;
import org.jboss.as.cli.handlers.CommandHandlerWithArguments;
import org.jboss.as.cli.impl.ArgumentWithoutValue;
import org.jboss.as.cli.util.HelpFormatter;
import org.jboss.as.protocol.StreamUtils;
import org.wildfly.security.manager.WildFlySecurityManager;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Basis for all CLI commands
 */
public abstract class CliCommandHandler extends CommandHandlerWithArguments {
   protected final CacheCommand cacheCommand;
   protected final ArgumentWithoutValue help;

   protected CliCommandHandler(CacheCommand cacheCommand) {
      super();
      this.cacheCommand = cacheCommand;
      help = new ArgumentWithoutValue(this, "--help", "-h");
      help.setExclusive(true);
   }

   protected void printHelp(CommandContext ctx) throws CommandLineException {
      String filename = "help/" + cacheCommand.getName() + ".txt";
      InputStream helpInput = WildFlySecurityManager.getClassLoaderPrivileged(NoArgumentsCliCommandHandler.class).getResourceAsStream(filename);
      if(helpInput != null) {
         BufferedReader reader = new BufferedReader(new InputStreamReader(helpInput));
         try {
            HelpFormatter.format(ctx, reader);
         } catch(java.io.IOException e) {
            throw new CommandFormatException("Failed to read " + filename +": " + e.getLocalizedMessage());
         } finally {
            StreamUtils.safeClose(reader);
         }
      } else {
         throw new CommandFormatException("Failed to locate command description " + filename);
      }
   }

   @Override
   public final void handle(CommandContext ctx) throws CommandLineException {
      recognizeArguments(ctx);
      if (help.isPresent(ctx.getParsedCommandLine())) {
         printHelp(ctx);
      } else {
         cliHandle(ctx);
      }
   }

   protected abstract void cliHandle(CommandContext ctx) throws CommandLineException;
}
