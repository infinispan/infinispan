package org.infinispan.client.hotrod.query.testdomain.protobuf;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class AnalyzerTestEntity {

   public String f1;

   public Integer f2;

   public AnalyzerTestEntity(String f1, Integer f2) {
      this.f1 = f1;
      this.f2 = f2;
   }

   @Override
   public String toString() {
      return "AnalyzerTestEntity{f1='" + f1 + "', f2=" + f2 + '}';
   }
}
