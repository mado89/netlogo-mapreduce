package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.nlogo.workspace.AbstractWorkspace;

/**
 * A helper class to get the world from the workspace
 * @see fillIn
 * @author Martin Dobiasch
 */
public class WorldSem
{
	private String world= "";
	private final Object sync= new Object();
	private boolean exportRunning= false;
	private AbstractWorkspace ws;
	
	/**
	 * This method has to be called before the world can be read via getWorld
	 * @see getWorld
	 */
	public void fillIn(AbstractWorkspace workspace)
	{
		exportRunning= true;
		world= "";
		this.ws= workspace;
		org.nlogo.awt.EventQueue.invokeLater(new Runnable()
		{
			public void run()
			{
				System.out.println("World export started");
				try
				{
					synchronized(sync)
					{
						StringWriter sw = new StringWriter();
						ws.exportWorld(new PrintWriter(sw));
						world = sw.toString();
						exportRunning= false;
						// System.out.println("World exported!" + world);
						sync.notifyAll();
					}
				}
				catch(IOException io)
				{
					io.printStackTrace();
				}
			}
		});
	}
	
	/*public void fillIn()
	{
		fillIn(Manager.em.workspace());
	}*/
	
	public String getWorld()
	{
		synchronized( sync )
		{
			while( exportRunning || world.equals("") )
			{
				System.out.println("Waiting for World export eR: " + exportRunning + " " + world.equals(""));
				try {
					sync.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return world;
	}
}