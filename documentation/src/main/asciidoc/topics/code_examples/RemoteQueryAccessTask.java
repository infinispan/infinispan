package org.infinispan.server.functional.extensions;

import java.util.Map;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.query.RemoteQueryAccess;

public class RemoteQueryAccessTask implements ServerTask<Integer> {

   private static final ThreadLocal<TaskContext> taskContext = new ThreadLocal<>();
   private static final String QUERY = "FROM pro.User WHERE name = :name order by id";
   private static final String QUERY_PROJ = "select id, name, surname " + QUERY;

   @Override
   public void setTaskContext(TaskContext ctx) {
      taskContext.set(ctx);
   }

   @Override
   public Integer call() {
      TaskContext ctx = taskContext.get();
      String name = (String) ctx.getParameters().get().get("name");
      RemoteQueryAccess remoteQueryAccess = ctx.getRemoteQueryAccess().get();

      Map<String, Object> params = Map.of("name", name);
      Query<User> query = remoteQueryAccess.query(QUERY);
      query.setParameters(params);
      QueryResult<User> result1 = query.execute();

      Query<Object[]> queryProj = remoteQueryAccess.query(QUERY_PROJ);
      query.setParameters(params);
      QueryResult<Object[]> result2 = queryProj.execute();

      return result1.count().value() + result2.count().value();
   }

   @Override
   public String getName() {
      return "remote-query-access-task";
   }
}
