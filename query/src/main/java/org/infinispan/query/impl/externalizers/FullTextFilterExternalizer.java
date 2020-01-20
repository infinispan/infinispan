package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.filter.impl.FullTextFilterImpl;
import org.infinispan.commons.marshall.AbstractExternalizer;

/**
 * @since 10.0
 */
public class FullTextFilterExternalizer extends AbstractExternalizer<FullTextFilterImpl> {
   @Override
   public void writeObject(ObjectOutput output, FullTextFilterImpl object) throws IOException {
      output.writeUTF(object.getName());
      Map<String, Object> parameters = object.getParameters();
      output.writeShort(parameters.size());
      for (Map.Entry<String, Object> e : parameters.entrySet()) {
         output.writeUTF(e.getKey());
         output.writeObject(e.getValue());
      }
   }

   @Override
   public FullTextFilterImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      String name = input.readUTF();
      FullTextFilterImpl fullTextFilter = new FullTextFilterImpl();
      fullTextFilter.setName(name);
      short paramSize = input.readShort();
      for (int i = 0; i < paramSize; i++) {
         fullTextFilter.setParameter(input.readUTF(), input.readObject());
      }
      return fullTextFilter;
   }

   @Override
   public Set<Class<? extends FullTextFilterImpl>> getTypeClasses() {
      return Collections.singleton(FullTextFilterImpl.class);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.FULL_TEXT_FILTER;
   }
}
