package org.infinispan.test.op;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.ValueMatcher;

public interface TestOperation {
   Class<? extends VisitableCommand> getCommandClass();

   Class<? extends ReplicableCommand> getBackupCommandClass();

   Object getValue();

   Object getPreviousValue();

   Object getReturnValue();

   void insertPreviousValue(AdvancedCache<Object, Object> cache, Object key);

   Object perform(AdvancedCache<Object, Object> cache, Object key);

   CompletionStage<?> performAsync(AdvancedCache<Object, Object> cache, Object key);

   ValueMatcher getValueMatcher();

   Object getReturnValueWithRetry();
}
