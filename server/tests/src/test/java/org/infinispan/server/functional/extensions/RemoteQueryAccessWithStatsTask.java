package org.infinispan.server.functional.extensions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.query.RemoteQueryAccess;

public class RemoteQueryAccessWithStatsTask implements ServerTask<String> {

   private static final ThreadLocal<TaskContext> taskContext = new ThreadLocal<>();
   private static final String QUERY = "FROM sample_bank_account.User WHERE name = :name order by id";
   private static final String QUERY_PROJ_TEXT = "select id, name, surname " + QUERY;

   @Override
   public void setTaskContext(TaskContext ctx) {
      taskContext.set(ctx);
   }

   @Override
   public String call() throws Exception {
      TaskContext ctx = taskContext.get();
      String name = (String) ctx.getParameters().get().get("name");

      Cache<Integer, Object> cache = (Cache<Integer, Object>) ctx.getCache().get();
      RemoteQueryAccess remoteQueryAccess = ctx.getRemoteQueryAccess().get();
      Map<String, Object> params = Map.of("name", name);

      List<?> objects = remoteQueryAccess.executeQuery(QUERY, params, -1, -1, 1, false).list();
      int firstQuerySize = objects.size();

      for (int i = 0; i < objects.size(); i++) {
         cache.put(500 + i, objects.get(i));
      }

      objects = remoteQueryAccess.executeQuery(QUERY_PROJ_TEXT, params, -1, -1, 1, false).list();
      List<Object[]> proj = (List<Object[]>) objects;
      Json jsonProj = Json.array();
      proj.forEach(array -> jsonProj.asJsonList().add(Json.array(array)));

      SearchStatisticsSnapshot statisticsSnapshot = Search.getClusteredSearchStatistics(cache)
            .toCompletableFuture().get(10, TimeUnit.SECONDS);
      Json json = statisticsSnapshot.toJson();

      Json serverTaskInfo = Json.object();
      serverTaskInfo.set("query-result-size", firstQuerySize);
      serverTaskInfo.set("param-name", name);
      serverTaskInfo.set("projection-query-result", jsonProj);

      json.set("server-task", serverTaskInfo);
      return json.toPrettyString();
   }

   @Override
   public String getName() {
      return "remote-query-access-with-stats";
   }
}
