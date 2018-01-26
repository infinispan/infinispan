package org.infinispan.server.hotrod;

import java.util.concurrent.Executor;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;

import io.netty.channel.Channel;

public class TaskRequestProcessor extends BaseRequestProcessor {
   private final HotRodServer server;
   private final TaskManager taskManager;

   TaskRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor);
      this.server = server;
      this.taskManager = SecurityActions.getGlobalComponentRegistry(server.getCacheManager()).getComponent(TaskManager.class);
   }

   public void exec(CacheDecodeContext cdc) {
      ExecRequestContext execContext = (ExecRequestContext) cdc.operationDecodeContext;
      // TODO: could we store the marshaller in a final field?
      Marshaller marshaller;
      if (server.getMarshaller() != null) {
         marshaller = server.getMarshaller();
      } else {
         marshaller = new GenericJBossMarshaller();
      }
      TaskContext taskContext = new TaskContext()
            .marshaller(marshaller)
            .cache(cdc.cache())
            .parameters(execContext.getParams())
            .subject(cdc.subject);
      // TODO: TaskManager API is already asynchronous, though we cannot be sure that it won't block anywhere
      taskManager.runTask(execContext.getName(), taskContext).whenComplete((result, throwable) -> handleExec(cdc, result, throwable));
   }

   private void handleExec(CacheDecodeContext cdc, Object result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else {
         HotRodHeader h = cdc.header;
         writeResponse(new ExecResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId,
                     result == null ? new byte[]{} : (byte[]) result));
      }
   }
}
