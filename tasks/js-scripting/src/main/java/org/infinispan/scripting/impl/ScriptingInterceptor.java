package org.infinispan.scripting.impl;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.scripting.ScriptingManager;

/**
 * Intercepts updates to the script caches, extracts metadata and updates the compiled scripts
 * accordingly
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public final class ScriptingInterceptor extends BaseCustomAsyncInterceptor {

   private ScriptingManagerImpl scriptingManager;

   @Inject
   public void init(ScriptingManager scriptingManager) {
      this.scriptingManager = (ScriptingManagerImpl) scriptingManager;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      String name = (String) command.getKey();
      String script = (String) command.getValue();
      ScriptMetadata metadata = extractMetadata(name, script, command.getMetadata());
      return invokeNext(ctx, command);
      // TODO: understand if this is required?
      // return asyncInvokeNext(ctx, command, scriptingManager.compileScript(name, script, metadata).thenAccept(command::setMetadata));
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      // scriptingManager.compiledScripts.clear();
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      // scriptingManager.compiledScripts.remove(command.getKey());
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      String name = (String) command.getKey();
      String script = (String) command.getNewValue();
      ScriptMetadata metadata = extractMetadata(name, script, command.getMetadata());
      return invokeNext(ctx, command);
//      return asyncInvokeNext(ctx, command, command::setMetadata);
//               scriptingManager.compileScript(name, script, metadata).thenAccept(command::setMetadata));
   }

   private ScriptMetadata extractMetadata(String name, String script, Metadata metadata) {
      if (metadata instanceof ScriptMetadata) {
         return (ScriptMetadata) metadata;
      } else {
         return ScriptMetadataParser.parse(name, script).merge(metadata).build();
      }
   }
}
