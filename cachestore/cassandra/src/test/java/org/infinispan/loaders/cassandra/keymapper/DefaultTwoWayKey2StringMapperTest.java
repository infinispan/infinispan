package org.infinispan.loaders.cassandra.keymapper;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.keymapper.DefaultTwoWayKey2StringMapperTest")
public class DefaultTwoWayKey2StringMapperTest {
	
	public void testKeyMapper() {
		DefaultTwoWayKey2StringMapper mapper = new DefaultTwoWayKey2StringMapper();
		
		String skey = mapper.getStringMapping("k1");
		assert skey == "k1";
		
		skey = mapper.getStringMapping(Integer.valueOf(100));
		
		assert skey != "100";
		
		Integer i = (Integer) mapper.getKeyMapping(skey);
		assert i.intValue() == 100;
		
		skey = mapper.getStringMapping(Boolean.TRUE);
		
		assert !skey.equalsIgnoreCase("true");
		
		Boolean b = (Boolean) mapper.getKeyMapping(skey);
		
		assert b.booleanValue();
		
		skey = mapper.getStringMapping(Double.valueOf(3.141592d));
		
		assert skey != "3.141592";
		
		Double d = (Double) mapper.getKeyMapping(skey);
		
		assert d == 3.141592d;
	}

}
