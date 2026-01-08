package org.infinispan.cli.converters;

import org.infinispan.cli.resources.Resource;

public class ListFormatConverter extends EnumConverter<Resource.ListFormat> {
   public ListFormatConverter() {
      super(Resource.ListFormat.class);
   }
}
