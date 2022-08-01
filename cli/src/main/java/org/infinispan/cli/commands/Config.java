package org.infinispan.cli.commands;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.Context;
import org.infinispan.cli.activators.ConfigConversionAvailable;
import org.infinispan.cli.completers.ConfigPropertyCompleter;
import org.infinispan.cli.completers.MediaTypeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "config", description = "Configuration operations", groupCommands = {Config.Set.class, Config.Get.class, Config.Reset.class, Config.Convert.class})
public class Config extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      Context context = invocation.getContext();
      context.getProperties().forEach((k, v) -> invocation.printf("%s=%s\n", k, v));
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "set", description = "Sets a configuration property")
   public static class Set extends CliCommand {

      @Arguments(description = "The property name and value", required = true, completer = ConfigPropertyCompleter.class)
      List<String> args;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         Context context = invocation.getContext();
         switch (args.size()) {
            case 1:
               context.setProperty(args.get(0), null);
               break;
            case 2:
               context.setProperty(args.get(0), args.get(1));
               break;
            default:
               throw Messages.MSG.wrongArgumentCount(args.size());
         }

         context.saveProperties();
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "get", description = "Gets a configuration property")
   public static class Get extends CliCommand {

      @Argument(description = "The name of the property", required = true, completer = ConfigPropertyCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         Context context = invocation.getContext();
         invocation.printf("%s=%s\n", name, context.getProperty(name));
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "reset", description = "Resets all configuration properties to their default values")
   public static class Reset extends CliCommand {
      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         Context context = invocation.getContext();
         context.resetProperties();
         context.saveProperties();
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "convert", description = "Converts configuration to different formats.", activator = ConfigConversionAvailable.class)
   public static class Convert extends CliCommand {

      @Argument(description = "Specifies the path to a configuration file to convert. Uses standard input (stdin) if you do not specify a path.", completer = FileOptionCompleter.class)
      Resource input;

      @Option(description = "Specifies the path to the output configuration file. Uses standard output (stdout) if you do not specify a path.", completer = FileOptionCompleter.class, shortName = 'o')
      Resource output;

      @Option(description = "Sets the format of the output configuration.", required = true, completer = MediaTypeCompleter.class, shortName = 'f')
      String format;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         InputStream is = null;
         OutputStream os = null;
         try {
            ParserRegistry registry = new ParserRegistry();
            is = input == null ? System.in : new FileInputStream(input.getAbsolutePath());
            ConfigurationBuilderHolder holder = registry.parse(is, null, null); // Auto-detect type
            os = output == null ? System.out : new FileOutputStream(output.getAbsolutePath());
            Map<String, Configuration> configurations = new HashMap<>();
            for (Map.Entry<String, ConfigurationBuilder> configuration : holder.getNamedConfigurationBuilders().entrySet()) {
               configurations.put(configuration.getKey(), configuration.getValue().build());
            }
            MediaType mediaType;
            switch (MediaTypeCompleter.MediaType.valueOf(format.toUpperCase(Locale.ROOT))) {
               case XML:
                  mediaType = MediaType.APPLICATION_XML;
                  break;
               case YAML:
                  mediaType = MediaType.APPLICATION_YAML;
                  break;
               case JSON:
                  mediaType = MediaType.APPLICATION_JSON;
                  break;
               default:
                  throw new CommandException("Invalid output format: " + format);
            }
            try (ConfigurationWriter writer = ConfigurationWriter.to(os).withType(mediaType).clearTextSecrets(true).prettyPrint(true).build()) {
               registry.serialize(writer, holder.getGlobalConfigurationBuilder().build(), configurations);
            }
            return CommandResult.SUCCESS;
         } catch (FileNotFoundException e) {
            throw new CommandException(e);
         } finally {
            if (input != null) {
               Util.close(is);
            }
            if (output != null) {
               Util.close(os);
            }
         }
      }
   }
}
