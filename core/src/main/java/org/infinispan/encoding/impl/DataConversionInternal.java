package org.infinispan.encoding.impl;

import org.infinispan.encoding.DataConversion;

public final class DataConversionInternal {
   public static final DataConversion IDENTITY_KEY = new DataConversion(true);
   public static final DataConversion IDENTITY_VALUE = new DataConversion(false);
}
