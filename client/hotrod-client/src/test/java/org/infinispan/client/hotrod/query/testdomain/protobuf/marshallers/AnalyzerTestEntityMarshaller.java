package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AnalyzerTestEntity;
import org.infinispan.protostream.MessageMarshaller;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class AnalyzerTestEntityMarshaller implements MessageMarshaller<AnalyzerTestEntity> {

   @Override
   public AnalyzerTestEntity readFrom(ProtoStreamReader reader) throws IOException {
      String f1 = reader.readString("f1");
      Integer f2 = reader.readInt("f2");
      return new AnalyzerTestEntity(f1, f2);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AnalyzerTestEntity analyzerTestEntity) throws IOException {
      writer.writeString("f1", analyzerTestEntity.f1);
      writer.writeInt("f2", analyzerTestEntity.f2);
   }

   @Override
   public Class<AnalyzerTestEntity> getJavaClass() {
      return AnalyzerTestEntity.class;
   }

   @Override
   public String getTypeName() {
      return "sample_bank_account.AnalyzerTestEntity";
   }
}
