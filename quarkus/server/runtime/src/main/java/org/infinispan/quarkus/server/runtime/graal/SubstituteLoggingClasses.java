package org.infinispan.quarkus.server.runtime.graal;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.resources.LoggingResource;
import org.infinispan.server.Bootstrap;
import org.infinispan.server.BootstrapLogging;
import org.infinispan.server.Server;
import org.infinispan.server.core.admin.AdminServerTask;
import org.infinispan.server.tasks.admin.LoggingRemoveTask;
import org.infinispan.server.tasks.admin.LoggingSetTask;
import org.infinispan.server.tasks.admin.ServerAdminOperationsHandler;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class SubstituteLoggingClasses {
}

@TargetClass(RestServer.class)
final class Target_RestServer {
   @Substitute
   private void registerLoggingResource(ResourceManager resourceManager, String restContext) {
      // Do nothing
   }
}

@Delete
@TargetClass(LoggingResource.class)
final class Target_LoggingResource {

}

@TargetClass(Server.class)
final class Target_Server {
   @Substitute
   private void shutdownLog4jLogManager() {
      // Do nothing
   }
}

@TargetClass(Bootstrap.class)
final class Target_Bootstrap {
   @Substitute
   private static void staticInitializer() {
      // Do nothing
   }
   @Substitute
   protected void configureLogging() {
      // Do nothing
   }
}

@Delete
@TargetClass(BootstrapLogging.class)
final class Target_BootstrapLogging { }

@TargetClass(ServerAdminOperationsHandler.class)
final class Target_ServerAdminOperationsHandler {
   @Substitute
   private static AdminServerTask<?>[] generateTasks(ConfigurationBuilderHolder defaultsHolder) {
      return generateTasksWithoutLogging(defaultsHolder);
   }

   @Alias
   private static AdminServerTask<?>[] generateTasksWithoutLogging(ConfigurationBuilderHolder defaultsHolder) {
      return null;
   }
}

@Delete
@TargetClass(LoggingRemoveTask.class)
final class Target_LoggingRemoveTask { }

@Delete
@TargetClass(LoggingSetTask.class)
final class Target_LoggingSetTask  { }
