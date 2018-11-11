package sqlTOMongoDb;

import java.util.List;
import java.util.Map;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class DBObjectUtil {

	public static void fixJson(List lst, JsonFieldFixer fixer, String key) {
		for (int i = 0; i < lst.size(); i++) {
			Object object = lst.get(i);
			if (object instanceof Map)
				fixJson((Map) object, fixer, key);
			else if (object instanceof List)
				fixJson((List) object, fixer, key);
			else {
				if (fixer.needToFixValue(key)) {
					object = fixer.fixValue(object);
					lst.set(i, object);
				}
			}
		}
	}

	public static void fixJson(Map map, JsonFieldFixer fixer, String key) {

		String[] keySet = (String[]) map.keySet().toArray(new String[0]);
		for (int i = 0; i < keySet.length; i++) {
			// If i need to fix the key do this:
			if (fixer.needToFixKey(key + "." + keySet[i])) {
				// map.remove=Returns the value to which this map previously associated the key
				Object remove = map.remove(keySet[i]);
				keySet[i] = fixer.fixKey(keySet[i]);
				map.put(keySet[i], remove);
			}

			String canonicalKey = key + "." + keySet[i];
			Object object = map.get(keySet[i]);
			if (object instanceof Map)
				fixJson((Map) object, fixer, canonicalKey);
			else if (object instanceof List)
				fixJson((List) object, fixer, canonicalKey);
			else {
				if (fixer.needToFixValue(canonicalKey)) {
					// map.put=If the map previously contained a mapping for the key,
					// the old value is replaced by the specified value
					map.put(keySet[i], fixer.fixValue(object));
				}
			}
		}
	}

	public static Object getInnerField(String substring, Map<String, Object> data) {
	
		String[] split = substring.split("[.]");
		for (int i = 0; i < split.length; i++) {
			if (data == null || !(data instanceof Map<?, ?>))
				return null;
			data = (Map<String, Object>) data.get(split[i]);
		}
		return data;
	}

}
