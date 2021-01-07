package org.infinispan.sample;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField;

public class Author {
   @FullTextField
   String name;

   @FullTextField
   String surname;
}