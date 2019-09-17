package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.Gender;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class GenderMarshaller implements EnumMarshaller<Gender> {

   @Override
   public Class<Gender> getJavaClass() {
      return Gender.class;
   }

   @Override
   public String getTypeName() {
      return "sample_bank_account.User.Gender";
   }

   @Override
   public Gender decode(int enumValue) {
      switch (enumValue) {
         case 0:
            return Gender.MALE;
         case 1:
            return Gender.FEMALE;
      }
      return null;  // unknown value
   }

   @Override
   public int encode(Gender gender) {
      switch (gender) {
         case MALE:
            return 0;
         case FEMALE:
            return 1;
         default:
            throw new IllegalArgumentException("Unexpected User.Gender value : " + gender);
      }
   }
}
