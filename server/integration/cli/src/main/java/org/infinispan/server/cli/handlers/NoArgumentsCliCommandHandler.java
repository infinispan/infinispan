package org.infinispan.server.cli.handlers;

import org.infinispan.server.cli.CliInterpreterException;
import org.infinispan.server.cli.util.CliCommandBuffer;
import org.infinispan.server.cli.util.InfinispanUtil;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandFormatException;
import org.jboss.as.cli.CommandHandler;
import org.jboss.as.cli.CommandHandlerProvider;
import org.jboss.as.cli.CommandLineException;
import org.jboss.as.cli.handlers.CommandHandlerWithArguments;
import org.jboss.as.cli.impl.ArgumentWithoutValue;
import org.jboss.as.cli.util.HelpFormatter;
import org.jboss.as.protocol.StreamUtils;
import org.jboss.dmr.ModelNode;
import org.wildfly.security.manager.WildFlySecurityManager;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * It represents the no-arg Infinispan CLI command. It should be used as a base class for other commands
 * with arguments Infinispan CLI command.
 * <p/>
 * The Infinispan CLI command is only available when connected and the prefix contains the
 * {@code cache-container}.
 * <p/>
 * The commands are sent to the Infinispan interpreted to be processed and the result is printed.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class NoArgumentsCliCommandHandler extends CommandHandlerWithArguments {

   protected final CacheCommand cacheCommand;
   protected final CliCommandBuffer buffer;
   protected final ArgumentWithoutValue help;

   public NoArgumentsCliCommandHandler(CacheCommand cacheCommand, CliCommandBuffer buffer) {
      super();
      this.cacheCommand = cacheCommand;
      this.buffer = buffer;
      help = new ArgumentWithoutValue(this, "--help", "-h");
      help.setExclusive(true);
   }

   @Override
   public boolean isAvailable(CommandContext ctx) {
      return ctx.getModelControllerClient() != null && InfinispanUtil.getCacheInfo(ctx).getContainer() != null;
   }

   @Override
   public boolean isBatchMode(CommandContext ctx) {
      return false;
   }

   @Override
   public void handle(CommandContext ctx) throws CommandLineException {
      recognizeArguments(ctx);
      if (help.isPresent(ctx.getParsedCommandLine())) {
         printHelp(ctx);
      } else if (buffer.append(buildCommandString(ctx), cacheCommand.getNesting())) {
         try {
            invokeCliRequestIfNeeded(ctx);
         } catch (CliInterpreterException e) {
            ctx.printLine(e.getLocalizedMessage());
         }
      }
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


   protected void printResult(ModelNode result, CommandContext context) throws CommandLineException {
      if (result == null || !result.has("result")) {
         return;
      }
      context.printLine(result.get("result").asString());
   }

   private void invokeCliRequestIfNeeded(CommandContext context) throws CommandLineException, CliInterpreterException {
      printResult(InfinispanUtil.cliRequest(context, buffer.getCommandAndReset()), context);
   }

   private String buildCommandString(CommandContext ctx) {
      StringBuilder command = new StringBuilder(cacheCommand.getName());
      if (ctx.getArgumentsString() != null) {
         command.append(' ').append(ctx.getArgumentsString());
      }
      command.append('\n');
      return command.toString();
   }

   public static class AbortProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new NoArgumentsCliCommandHandler(CacheCommand.ABORT, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.ABORT.getName() };
      }

   }

   public static class EndProvider implements CommandHandlerProvider {

      @Override
      public CommandHandler createCommandHandler(CommandContext ctx) {
         return new NoArgumentsCliCommandHandler(CacheCommand.END, CliCommandBuffer.INSTANCE);
      }

      @Override
      public boolean isTabComplete() {
         return true;
      }

      @Override
      public String[] getNames() {
         return new String[] { CacheCommand.END.getName() };
      }
   }


}
