package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
public class AnalyzerTestEntity {

   @ProtoField(1)
   @Text(projectable = true, analyzer = "stemmer")
   final String f1;

   @Basic(indexNullAs = "-1")
   @ProtoField(2)
   public Integer f2;

   @ProtoFactory
   public AnalyzerTestEntity(String f1, Integer f2) {
      this.f1 = f1;
      this.f2 = f2;
   }

   @Override
   public String toString() {
      return "AnalyzerTestEntity{f1='" + f1 + "', f2=" + f2 + '}';
   }
}
