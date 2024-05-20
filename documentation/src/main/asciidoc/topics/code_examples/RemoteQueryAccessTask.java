package org.infinispan.server.functional.extensions;

import java.util.Map;

import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.query.RemoteQueryAccess;

public class RemoteQueryAccessTask implements ServerTask<Integer> {

   private static final ThreadLocal<TaskContext> taskContext = new ThreadLocal<>();
   private static final String QUERY = "FROM pro.User WHERE name = :name order by id";
   private static final String QUERY_PROJ_TEXT = "select id, name, surname " + QUERY;

   @Override
   public void setTaskContext(TaskContext ctx) {
      taskContext.set(ctx);
   }

   @Override
   public Integer call() throws Exception {
      TaskContext ctx = taskContext.get();
      String name = (String) ctx.getParameters().get().get("name");
      RemoteQueryAccess remoteQueryAccess = ctx.getRemoteQueryAccess().get();

      Map<String, Object> params = Map.of("name", name);
      QueryResult<?> result1 = remoteQueryAccess.executeQuery(QUERY, params, 0, 10, 100, false);
      QueryResult<?> result2 = remoteQueryAccess.executeQuery(QUERY_PROJ_TEXT, params, 0, 10, 100, false);
      return result1.count().value() + result2.count().value();
   }

   @Override
   public String getName() {
      return "remote-query-access";
   }
}
