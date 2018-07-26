package org.infinispan.server.infinispan.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.scripting.utils.ScriptConversions;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskManager;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 12:53 PM
 */
@Scope(Scopes.GLOBAL)
public class ServerTaskRegistryImpl implements ServerTaskRegistry {

   private ConcurrentMap<String, ServerTaskWrapper> tasks = new ConcurrentHashMap<>();

   @Inject
   public void init(TaskManager taskManager, EmbeddedCacheManager cacheManager) {
      EncoderRegistry encoderRegistry = cacheManager.getGlobalComponentRegistry().getComponent(EncoderRegistry.class);
      ServerTaskEngine engine = new ServerTaskEngine(this, cacheManager, new ScriptConversions(encoderRegistry));
      taskManager.registerTaskEngine(engine);
   }

   @Override
   public List<Task> getTasks() {
      Collection<ServerTaskWrapper> tasks = this.tasks.values();
      return new ArrayList<>(tasks);
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> ServerTaskWrapper<T> getTask(String taskName) {
      return (ServerTaskWrapper<T>) tasks.get(taskName);
   }

   @Override
   public boolean handles(String taskName) {
      return tasks.containsKey(taskName);
   }

   @Override
   public <T> void addDeployedTask(ServerTask<T> task) {
      ServerTaskWrapper taskWrapper = new ServerTaskWrapper<>(task);
      tasks.put(task.getName(), taskWrapper);
   }

   @Override
   public void removeDeployedTask(String name) {
      tasks.remove(name);
   }
}
