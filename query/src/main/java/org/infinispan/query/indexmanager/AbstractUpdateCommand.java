package org.infinispan.query.indexmanager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.util.ByteString;

/**
 * Base class for index commands.
 *
 * @author gustavonalle
 * @since 7.0
 */
public abstract class AbstractUpdateCommand extends BaseRpcCommand implements ReplicableCommand {

   protected String indexName;
   protected byte[] serializedModel;

   protected AbstractUpdateCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      if (indexName == null) {
         output.writeBoolean(false);
      } else {
         output.writeBoolean(true);
         output.writeUTF(indexName);
      }
      MarshallUtil.marshallByteArray(serializedModel, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException {
      boolean hasIndexName = input.readBoolean();
      if (hasIndexName) {
         indexName = input.readUTF();
      }
      serializedModel = MarshallUtil.unmarshallByteArray(input);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   public String getIndexName() {
      return indexName;
   }

   protected void setSerializedWorkList(byte[] serializedModel) {
      this.serializedModel = serializedModel;
   }

   public void setIndexName(String indexName) {
      this.indexName = indexName;
   }
}
