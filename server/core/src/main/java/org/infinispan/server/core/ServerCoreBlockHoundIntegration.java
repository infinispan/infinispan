package org.infinispan.server.core;

import org.infinispan.commons.internal.CommonsBlockHoundIntegration;
import org.kohsuke.MetaInfServices;

import io.netty.util.concurrent.GlobalEventExecutor;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerCoreBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "addTask");
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "takeTask");

      // The xerces parser when it finds a parsing error will print to possibly a file output - ignore
      builder.allowBlockingCallsInside("com.sun.org.apache.xerces.internal.util.DefaultErrorHandler", "printError");
      // Nashorn prints to stderr in its constructor
      builder.allowBlockingCallsInside("jdk.nashorn.api.scripting.NashornScriptEngineFactory", "getScriptEngine");

      methodsToBeRemoved(builder);
   }

   /**
    * Various methods that need to be removed as they are essentially bugs. Please ensure that a JIRA is created and
    * referenced here for any such method
    * @param builder the block hound builder to register methods
    */
   private static void methodsToBeRemoved(BlockHound.Builder builder) {
      // ProtobufMetadataManagerInterceptor is blocking in quite a few places
      // https://issues.redhat.com/browse/ISPN-11832
      try {
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, Class.forName("org.infinispan.query.remote.impl.ProtobufMetadataManagerInterceptor"));
      } catch (ClassNotFoundException e) {
         // Just ignore - means that most likely this module isn't present (ie. server/core or server/memcached)
      }

      // ScriptingManger interface is blocking - but relies upon a persistent and clustered cache
      // https://issues.redhat.com/browse/ISPN-11833
      try {
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, Class.forName("org.infinispan.scripting.impl.ScriptingManagerImpl"));
      } catch (ClassNotFoundException e) {
         // Just ignore - means that most likely this module isn't present (ie. server/core or server/memcached)
      }

      // Counter creation is blocking
      // https://issues.redhat.com/browse/ISPN-11434
      builder.allowBlockingCallsInside("org.infinispan.counter.impl.manager.EmbeddedCounterManager", "createCounter");
   }
}
