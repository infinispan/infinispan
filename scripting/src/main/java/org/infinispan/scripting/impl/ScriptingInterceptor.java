package org.infinispan.scripting.impl;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BaseCustomSequentialInterceptor;
import org.infinispan.scripting.ScriptingManager;

import java.util.concurrent.CompletableFuture;

/**
 * Intercepts updates to the script caches, extracts metadata and updates the compiled scripts
 * accordingly
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public final class ScriptingInterceptor extends BaseCustomSequentialInterceptor {

   private ScriptingManagerImpl scriptingManager;

   @Inject
   public void init(ScriptingManager scriptingManager) {
      this.scriptingManager = (ScriptingManagerImpl) scriptingManager;
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      String name = (String) command.getKey();
      String script = (String) command.getValue();
      command.setMetadata(scriptingManager.compileScript(name, script));
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      scriptingManager.compiledScripts.clear();
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      scriptingManager.compiledScripts.remove(command.getKey());
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      String name = (String) command.getKey();
      String script = (String) command.getNewValue();
      command.setMetadata(scriptingManager.compileScript(name, script));
      return ctx.continueInvocation();
   }

}
