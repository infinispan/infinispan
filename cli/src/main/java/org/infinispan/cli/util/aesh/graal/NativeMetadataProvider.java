package org.infinispan.cli.util.aesh.graal;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.aesh.command.Command;
import org.aesh.command.container.CommandContainer;
import org.aesh.command.container.CommandContainerBuilder;
import org.aesh.command.impl.container.AeshCommandContainerBuilder;
import org.aesh.command.impl.internal.ProcessedCommand;
import org.aesh.command.impl.internal.ProcessedOption;
import org.aesh.command.impl.parser.CommandLineParser;
import org.aesh.command.invocation.CommandInvocation;
import org.infinispan.commons.graalvm.ReflectiveClass;

public class NativeMetadataProvider implements org.infinispan.commons.graalvm.NativeMetadataProvider {

   final List<ReflectiveClass> classes = new ArrayList<>();

   public NativeMetadataProvider() {
      loadCommand(org.infinispan.cli.commands.Batch.class);
      loadCommand(org.infinispan.cli.commands.CLI.class);
      loadCommand(org.infinispan.cli.commands.kubernetes.Kube.class);
      classes.addAll(List.of(
            ReflectiveClass.of(org.infinispan.cli.impl.ExitCodeResultHandler.class),
            ReflectiveClass.of(org.infinispan.cli.logging.Messages_$bundle.class),
            ReflectiveClass.of(org.infinispan.commons.logging.Log_$logger.class),
            ReflectiveClass.of(org.aesh.command.impl.parser.AeshOptionParser.class),
            ReflectiveClass.of(org.apache.logging.log4j.core.impl.Log4jContextFactory.class),
            ReflectiveClass.of(org.apache.logging.log4j.core.util.ExecutorServices.class),
            ReflectiveClass.of(org.apache.logging.log4j.message.ParameterizedMessageFactory.class),
            ReflectiveClass.of(org.apache.logging.log4j.message.DefaultFlowMessageFactory.class),
            ReflectiveClass.of(org.wildfly.security.password.impl.PasswordFactorySpiImpl.class)
      ));
   }

   @Override
   public Stream<ReflectiveClass> reflectiveClasses() {
      return classes.stream();
   }

   @SuppressWarnings("unchecked")
   private void loadCommand(Class<?> cmd) {
      Class<Command<CommandInvocation<?>>> clazz = (Class<Command<CommandInvocation<?>>>) cmd;
      CommandContainerBuilder<CommandInvocation<?>> builder = new AeshCommandContainerBuilder<>();
      try (CommandContainer<CommandInvocation<?>> container = builder.create(clazz)) {
         addCommandClasses(container.getParser());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private void addCommandClasses(CommandLineParser<CommandInvocation<?>> parser) {
      addCommandClasses(parser.getProcessedCommand());
      if (parser.isGroupCommand()) {
         for (CommandLineParser<CommandInvocation<?>> child : parser.getAllChildParsers())
            addCommandClasses(child);
      }
   }

   private void addCommandClasses(ProcessedCommand<Command<CommandInvocation<?>>, CommandInvocation<?>> command) {
      Class<?> clazz = command.getCommand().getClass();
      if (command.getActivator() != null)
         classes.add(ReflectiveClass.of(command.getActivator().getClass()));

      List<ProcessedOption> allArgumentsAndOptions = new ArrayList<>(command.getOptions());
      if (command.getArguments() != null)
         allArgumentsAndOptions.add(command.getArguments());

      if (command.getArgument() != null)
         allArgumentsAndOptions.add(command.getArgument());

      Field[] fields = new Field[allArgumentsAndOptions.size()];
      for (int i = 0; i < fields.length; i++) {
         ProcessedOption option = allArgumentsAndOptions.get(i);
         try {
            fields[i] = getField(clazz, option.getFieldName());
         } catch (NoSuchFieldException e) {
            throw new RuntimeException(String.format("Unable to process command '%s'", clazz.getName()), e);
         }

         if (option.completer() != null)
            classes.add(ReflectiveClass.of(option.completer().getClass(), false, true));

         if (option.activator() != null)
            classes.add(ReflectiveClass.of(option.activator().getClass(), false, true));

         if (option.converter() != null)
            classes.add(ReflectiveClass.of(option.converter().getClass(), false, true));
      }

      classes.add(
            new ReflectiveClass(clazz, clazz.getDeclaredConstructors(), fields, clazz.getDeclaredMethods())
      );
   }

   Field getField(Class<?> clazz, String field) throws NoSuchFieldException {
      try {
         return clazz.getDeclaredField(field);
      } catch (NoSuchFieldException e) {
         if (clazz.getSuperclass().equals(Object.class))
            throw e;
         return getField(clazz.getSuperclass(), field);
      }
   }
}
