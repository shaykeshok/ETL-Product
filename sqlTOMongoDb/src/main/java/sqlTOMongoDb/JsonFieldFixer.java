package sqlTOMongoDb;

public interface JsonFieldFixer {

	public boolean needToFixValue(String key);
	public Object fixValue(Object object);
	public boolean needToFixKey(String string);
	public String fixKey(String string);

}
