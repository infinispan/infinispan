package org.infinispan.server.hotrod;

import java.util.Map;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;

import io.netty.channel.Channel;

public class TaskRequestProcessor extends BaseRequestProcessor {
   private final HotRodServer server;
   private final TaskManager taskManager;

   TaskRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);
      this.server = server;
      this.taskManager = SecurityActions.getGlobalComponentRegistry(server.getCacheManager()).getComponent(TaskManager.class);
   }

   public void exec(HotRodHeader header, Subject subject, String taskName, Map<String, byte[]> taskParams) {
      // TODO: could we store the marshaller in a final field?
      Marshaller marshaller;
      if (server.getMarshaller() != null) {
         marshaller = server.getMarshaller();
      } else {
         marshaller = new GenericJBossMarshaller(server.getCacheManager().getClassWhiteList());
      }
      AdvancedCache<byte[], byte[]> cache = server.cache(header, subject);
      TaskContext taskContext = new TaskContext()
            .marshaller(marshaller)
            .cache(cache)
            .parameters(taskParams)
            .subject(subject);
      // TODO: TaskManager API is already asynchronous, though we cannot be sure that it won't block anywhere
      taskManager.runTask(taskName, taskContext).whenComplete((result, throwable) -> handleExec(header, result, throwable));
   }

   private void handleExec(HotRodHeader header, Object result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         writeResponse(header, header.encoder().valueResponse(header, server, channel.alloc(), OperationStatus.Success,
               result == null ? Util.EMPTY_BYTE_ARRAY : (byte[]) result));
      }
   }
}
