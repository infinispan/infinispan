package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.ReflectionUtil;

public class PojoRawTypeIdentifierExternalizer extends AbstractExternalizer<PojoRawTypeIdentifier> {

   @Override
   public Integer getId() {
      return ExternalizerIds.POJO_TYPE_IDENTIFIER;
   }

   @Override
   public Set<Class<? extends PojoRawTypeIdentifier>> getTypeClasses() {
      return Collections.singleton(PojoRawTypeIdentifier.class);
   }

   @Override
   public void writeObject(ObjectOutput output, PojoRawTypeIdentifier object) throws IOException {
      output.writeObject(object.javaClass());
      if (object.isNamed()) {
         // TODO avoid reflection here
         String name = (String) ReflectionUtil.getValue(object, "name");
         output.writeObject(Optional.of(name));
      } else {
         output.writeObject(Optional.empty());
      }
   }

   @Override
   public PojoRawTypeIdentifier readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Class<?> javaClass = (Class<?>) input.readObject();
      Optional<String> typeName = (Optional<String>) input.readObject();
      return (typeName.isPresent()) ? PojoRawTypeIdentifier.of(javaClass, typeName.get()) :
            PojoRawTypeIdentifier.of(javaClass);
   }
}
