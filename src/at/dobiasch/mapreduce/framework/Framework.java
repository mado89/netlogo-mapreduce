package at.dobiasch.mapreduce.framework;


public class Framework
{
	private Configuration config;
	
	public Framework()
	{
		config= new Configuration();
	}
	
	public Configuration getConfiguration()
	{
		return config;
	}
}
