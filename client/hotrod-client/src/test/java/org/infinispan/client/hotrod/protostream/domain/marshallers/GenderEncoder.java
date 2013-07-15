package org.infinispan.client.hotrod.protostream.domain.marshallers;

import org.infinispan.client.hotrod.protostream.domain.User;
import org.infinispan.protostream.EnumEncoder;

/**
 * @author anistor@redhat.com
 */
public class GenderEncoder implements EnumEncoder<User.Gender> {

   @Override
   public String getFullName() {
      return "sample_bank_account.User.Gender";
   }

   @Override
   public User.Gender decode(int enumValue) {
      switch (enumValue) {
         case 0:
            return User.Gender.MALE;
         case 1:
            return User.Gender.FEMALE;
      }
      return null;
   }

   @Override
   public int encode(User.Gender gender) {
      return gender == User.Gender.MALE ? 0 : 1;
   }
}
