package org.infinispan.remoting;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.transport.Address;

import java.util.concurrent.Callable;

/**
 * Simulates a remote invocation on the local node. This is needed because the transport does not redirect to itself the
 * replicable commands.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class LocalInvocation implements Callable<Response> {

   private final ResponseGenerator responseGenerator;
   private final CacheRpcCommand command;
   private final CommandsFactory commandsFactory;
   private final Address self;

   private LocalInvocation(ResponseGenerator responseGenerator, CacheRpcCommand command,
                           CommandsFactory commandsFactory, Address self) {
      this.responseGenerator = responseGenerator;
      this.command = command;
      this.commandsFactory = commandsFactory;
      this.self = self;
   }

   @Override
   public Response call() throws Exception {
      try {
         commandsFactory.initializeReplicableCommand(command, false);
         command.setOrigin(self);
         return responseGenerator.getResponse(command, command.perform(null));
      } catch (Throwable throwable) {
         return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
      }
   }

   public static LocalInvocation newInstanceFromCache(Cache<Object, Object> cache, CacheRpcCommand command) {
      ComponentRegistry registry = cache.getAdvancedCache().getComponentRegistry();
      ResponseGenerator responseGenerator = registry.getResponseGenerator();
      CommandsFactory commandsFactory = registry.getCommandsFactory();
      Address self = registry.getComponent(ClusteringDependentLogic.class).getAddress();
      return newInstance(responseGenerator, command, commandsFactory, self);
   }

   public static LocalInvocation newInstance(ResponseGenerator responseGenerator, CacheRpcCommand command,
                                             CommandsFactory commandsFactory, Address self) {
      if (responseGenerator == null || command == null || commandsFactory == null || self == null) {
         throw new NullPointerException("Null arguments are not allowed.");
      }
      return new LocalInvocation(responseGenerator, command, commandsFactory, self);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LocalInvocation that = (LocalInvocation) o;

      return command.equals(that.command);

   }

   @Override
   public int hashCode() {
      return command.hashCode();
   }
}
