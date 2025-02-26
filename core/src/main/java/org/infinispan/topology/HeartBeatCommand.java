package org.infinispan.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A hear-beat command used to ping members in {@link ClusterTopologyManagerImpl#confirmMembersAvailable()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.HEART_BEAT_COMMAND)
public class HeartBeatCommand implements GlobalRpcCommand {

   public static final HeartBeatCommand INSTANCE = new HeartBeatCommand();

   @ProtoFactory
   static HeartBeatCommand protoFactory() {
      return INSTANCE;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
