package at.dobiasch.mapreduce.framework;

import org.nlogo.api.ExtensionException;


public class FrameworkFactory
{
	private static Framework inst= null;
	
	private FrameworkFactory()
	{
		
	}
	
	public static synchronized Framework getInstance() throws ExtensionException
	{
        if (inst == null)
        {
        	inst = new Framework();
        	System.out.println("new Framework");
        }
        return inst;
    }
}
