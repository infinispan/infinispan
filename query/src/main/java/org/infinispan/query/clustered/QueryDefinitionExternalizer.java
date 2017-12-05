package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.query.QueryDefinition;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

public class QueryDefinitionExternalizer implements AdvancedExternalizer<QueryDefinition> {

   @Override
   public Set<Class<? extends QueryDefinition>> getTypeClasses() {
      return Collections.singleton(QueryDefinition.class);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.QUERY_DEFINITION;
   }

   @Override
   public void writeObject(ObjectOutput output, QueryDefinition object) throws IOException {
      if (object.getQueryString().isPresent()) {
         output.writeBoolean(true);
         output.writeUTF(object.getQueryString().get());
      } else {
         output.writeBoolean(false);
         output.writeObject(object.getHsQuery());
      }
      output.writeInt(object.getFirstResult());
      output.writeInt(object.getMaxResults());
   }

   @Override
   public QueryDefinition readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      QueryDefinition queryDefinition;
      if (input.readBoolean()) {
         queryDefinition = new QueryDefinition(input.readUTF());
      } else {
         queryDefinition = new QueryDefinition((HSQuery) input.readObject());
      }
      queryDefinition.setFirstResult(input.readInt());
      queryDefinition.setMaxResults(input.readInt());
      return queryDefinition;
   }
}
