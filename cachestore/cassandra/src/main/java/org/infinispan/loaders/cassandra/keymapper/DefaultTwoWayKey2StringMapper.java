package org.infinispan.loaders.cassandra.keymapper;

/**
 * Default implementation for {@link TwoWayKey2StringMapper} that knows how to
 * handle all primitive wrapper keys and Strings.
 * 
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * 
 * @since 4.1
 */
public class DefaultTwoWayKey2StringMapper implements TwoWayKey2StringMapper {
	private static final char NON_STRING_PREFIX = '\uFEFF';
	private static final char SHORT_IDENTIFIER = '1';
	private static final char BYTE_IDENTIFIER = '2';
	private static final char LONG_IDENTIFIER = '3';
	private static final char INTEGER_IDENTIFIER = '4';
	private static final char DOUBLE_IDENTIFIER = '5';
	private static final char FLOAT_IDENTIFIER = '6';
	private static final char BOOLEAN_IDENTIFIER = '7';

	@Override
	public String getStringMapping(Object key) {
		char identifier;
		if (key.getClass().equals(String.class)) {
			return key.toString();
		} else if (key.getClass().equals(Short.class)) {
			identifier = SHORT_IDENTIFIER;
		} else if (key.getClass().equals(Byte.class)) {
			identifier = BYTE_IDENTIFIER;
		} else if (key.getClass().equals(Long.class)) {
			identifier = LONG_IDENTIFIER;
		} else if (key.getClass().equals(Integer.class)) {
			identifier = INTEGER_IDENTIFIER;
		} else if (key.getClass().equals(Double.class)) {
			identifier = DOUBLE_IDENTIFIER;
		} else if (key.getClass().equals(Float.class)) {
			identifier = FLOAT_IDENTIFIER;
		} else if (key.getClass().equals(Boolean.class)) {
			identifier = BOOLEAN_IDENTIFIER;
		} else {
			throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
		}
		return generateString(identifier, key.toString());
	}

	@Override
	public Object getKeyMapping(String key) {
		if (key.charAt(0) == NON_STRING_PREFIX) {
			char type = key.charAt(1);
			String value = key.substring(2);
			switch (type) {
			case SHORT_IDENTIFIER:
				return Short.parseShort(value);
			case BYTE_IDENTIFIER:
				return Byte.parseByte(value);
			case LONG_IDENTIFIER:
				return Long.parseLong(value);
			case INTEGER_IDENTIFIER:
				return Integer.parseInt(value);
			case DOUBLE_IDENTIFIER:
				return Double.parseDouble(value);
			case FLOAT_IDENTIFIER:
				return Float.parseFloat(value);
			case BOOLEAN_IDENTIFIER:
				return Boolean.parseBoolean(value);
			default:
				throw new IllegalArgumentException("Unsupported type code: " + type);
			}
		} else {
			return key;
		}
	}

	@Override
	public boolean isSupportedType(Class<?> keyType) {
		return isPrimitive(keyType);
	}

	private String generateString(char identifier, String s) {
		return String.valueOf(NON_STRING_PREFIX) + String.valueOf(identifier) + s;
	}

	static boolean isPrimitive(Class<?> key) {
		return key == String.class || key == Short.class || key == Byte.class || key == Long.class || key == Integer.class || key == Double.class || key == Float.class || key == Boolean.class;
	}
}
