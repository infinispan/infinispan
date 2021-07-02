package org.infinispan.server.core;

import org.infinispan.server.core.utils.SslUtils;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@SuppressWarnings("unused")
@MetaInfServices
public class ServerCoreBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // The xerces parser when it finds a parsing error will print to possibly a file output - ignore
      builder.allowBlockingCallsInside("com.sun.org.apache.xerces.internal.util.DefaultErrorHandler", "printError");

      // XStream uses reflection that can incur in illegal access being logged to disk due to modules isolation
      builder.allowBlockingCallsInside("com.thoughtworks.xstream.converters.reflection.SerializableConverter", "isSerializable");

      // Nashorn prints to stderr in its constructor
      builder.allowBlockingCallsInside("jdk.nashorn.api.scripting.NashornScriptEngineFactory", "getScriptEngine");

      questionableBlockingMethod(builder);
      methodsToBeRemoved(builder);
   }

   private static void questionableBlockingMethod(BlockHound.Builder builder) {
      // Loads a file on ssl connect to read the key store
      builder.allowBlockingCallsInside(SslUtils.class.getName(), "createNettySslContext");
   }

   /**
    * Various methods that need to be removed as they are essentially bugs. Please ensure that a JIRA is created and
    * referenced here for any such method
    * @param builder the block hound builder to register methods
    */
   private static void methodsToBeRemoved(BlockHound.Builder builder) {
      // Counter creation is blocking
      // https://issues.redhat.com/browse/ISPN-11434
      builder.allowBlockingCallsInside("org.infinispan.counter.impl.manager.EmbeddedCounterManager", "createCounter");
   }
}
