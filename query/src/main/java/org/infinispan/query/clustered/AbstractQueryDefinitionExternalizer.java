package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.query.QueryDefinition;

/**
 * @since 9.2
 */
public abstract class AbstractQueryDefinitionExternalizer<Q extends QueryDefinition> implements AdvancedExternalizer<Q> {

   @Override
   public void writeObject(ObjectOutput output, Q object) throws IOException {
      if (object.getQueryString().isPresent()) {
         output.writeBoolean(true);
         output.writeUTF(object.getQueryString().get());
      } else {
         output.writeBoolean(false);
         output.writeObject(object.getHsQuery());
      }
      output.writeInt(object.getFirstResult());
      output.writeInt(object.getMaxResults());
      output.writeObject(object.getSortableFields());
      output.writeObject(object.getIndexedType());
      Map<String, Object> namedParameters = object.getNamedParameters();
      int size = namedParameters.size();
      output.writeShort(size);
      for (Map.Entry<String, Object> param : namedParameters.entrySet()) {
         output.writeUTF(param.getKey());
         output.writeObject(param.getValue());
      }
   }

   abstract protected Q createQueryDefinition(String q);

   abstract protected Q createQueryDefinition(HSQuery hsQuery);

   @Override
   public Q readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Q queryDefinition;
      if (input.readBoolean()) {
         queryDefinition = createQueryDefinition(input.readUTF());
      } else {
         queryDefinition = createQueryDefinition((HSQuery) input.readObject());
      }
      queryDefinition.setFirstResult(input.readInt());
      queryDefinition.setMaxResults(input.readInt());
      Set<String> sortableField = (Set<String>) input.readObject();
      Class<?> indexedTypes = (Class<?>) input.readObject();
      queryDefinition.setSortableField(sortableField);
      queryDefinition.setIndexedType(indexedTypes);
      short paramSize = input.readShort();
      if (paramSize > 0) {
         Map<String, Object> params = new HashMap<>();
         for (int i = 0; i < paramSize; i++) {
            String key = input.readUTF();
            Object value = input.readObject();
            params.put(key, value);
         }
         queryDefinition.setNamedParameters(params);
      }
      return queryDefinition;
   }
}
