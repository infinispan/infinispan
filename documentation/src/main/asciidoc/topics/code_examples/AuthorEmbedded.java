package org.infinispan.sample;

import org.infinispan.api.annotations.indexing.Text;

public class Author {
   @Text
   String name;

   @Text
   String surname;
}
