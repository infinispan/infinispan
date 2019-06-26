Cache noPreviousValueCache = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
noPreviousValueCache.put(k, v);
