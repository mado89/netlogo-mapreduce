package at.dobiasch.mapreduce.framework;


public class FrameworkFactory
{
	private static Framework inst= null;
	
	private FrameworkFactory()
	{
		
	}
	
	public static synchronized Framework getInstance()
	{
        if (inst == null)
        {
        	inst = new Framework();
        	System.out.println("new Framework");
        }
        return inst;
    }
}
