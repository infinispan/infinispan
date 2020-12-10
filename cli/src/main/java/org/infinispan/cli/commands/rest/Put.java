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
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.EncodingCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.client.rest.RestCacheClient;
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
@CommandDefinition(name = "put", description = "Puts an entry into the cache", activator = ConnectionActivator.class)
public class Put extends RestCliCommand {

   @Arguments(required = true)
   List<String> args;

   @Option(completer = EncodingCompleter.class, shortName = 'e')
   String encoding;

   @Option(completer = CacheCompleter.class, shortName = 'c')
   String cache;

   @Option(completer = FileOptionCompleter.class, shortName = 'f')
   Resource file;

   @Option(shortName = 'l', defaultValue = "0")
   long ttl;

   @Option(name = "max-idle", shortName = 'i', defaultValue = "0")
   long maxIdle;

   @Option(name = "if-absent", shortName = 'a')
   boolean ifAbsent;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, org.infinispan.cli.resources.Resource resource) {
      if ((file != null) && (args.size() != 1)) {
         throw Messages.MSG.illegalCommandArguments();
      } else if ((file == null) && (args.size() != 2)) {
         throw Messages.MSG.illegalCommandArguments();
      }

      RestCacheClient cacheClient = client.cache(cache != null ? cache : CacheResource.cacheName(resource));
      MediaType putEncoding = encoding != null ? MediaType.fromString(encoding) : invocation.getContext().getEncoding();
      RestEntity value = file != null ? RestEntity.create(putEncoding, new File(file.getAbsolutePath())) : RestEntity.create(putEncoding, args.get(1));
      if (ifAbsent) {
         return cacheClient.post(args.get(0), value, ttl, maxIdle);
      } else {
         return cacheClient.put(args.get(0), value, ttl, maxIdle);
      }
   }
}
