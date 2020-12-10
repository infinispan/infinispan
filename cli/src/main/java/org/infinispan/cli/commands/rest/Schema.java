package org.infinispan.cli.commands.rest;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "schema", description = "Manipulates protobuf schemas", activator = ConnectionActivator.class)
public class Schema extends RestCliCommand {
   @Arguments(required = true, description = "The name of the schema")
   List<String> args;

   @Option(completer = FileOptionCompleter.class, shortName = 'u', description = "The protobuf file to upload")
   Resource upload;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
      if ((upload != null) && (args.size() != 1)) {
         throw Messages.MSG.illegalCommandArguments();
      } else if ((upload == null) && (args.size() != 2)) {
         throw Messages.MSG.illegalCommandArguments();
      }
      if (upload == null) {
         return client.schemas().put(args.get(0), args.get(1));
      } else {
         return client.schemas().put(args.get(0), RestEntity.create(MediaType.TEXT_PLAIN, new File(upload.getAbsolutePath())));
      }
   }
}
