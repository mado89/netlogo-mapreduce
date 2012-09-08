package org.nlogo.extensions.mapreduce;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.nlogo.extensions.mapreduce.commands.AcceptWorkers;
import org.nlogo.extensions.mapreduce.commands.MapReduce;
import org.nlogo.extensions.mapreduce.commands.Node;
import org.nlogo.extensions.mapreduce.commands.NodeParams;
import org.nlogo.extensions.mapreduce.commands.PlotConfig;
import org.nlogo.extensions.mapreduce.commands.Test;
import org.nlogo.extensions.mapreduce.commands.Values;
import org.nlogo.extensions.mapreduce.commands.config.InputDir;
import org.nlogo.extensions.mapreduce.commands.config.Mapper;
import org.nlogo.extensions.mapreduce.commands.config.OutDir;
import org.nlogo.extensions.mapreduce.commands.config.Reducer;

/**
 * Manager Class for the extension
 * @author Martin Dobiasch
 */
public class Manager extends org.nlogo.api.DefaultClassManager
{
	public static org.nlogo.workspace.ExtensionManager em;
	private static WorldSem world;
	
	/**
	 * A helper class to get the world from the workspace
	 * @see fillIn
	 * @author Martin Dobiasch
	 */
	private class WorldSem
	{
		private String world= "";
		private final Object sync= new Object();
		private boolean exportRunning= false;
		
		/**
		 * This method has to be called before the world can be read via getWorld
		 * @see getWorld
		 */
		public void fillIn()
		{
			org.nlogo.awt.EventQueue.invokeLater(new Runnable()
			{
				public void run()
				{
					try
					{
						synchronized(sync)
						{
							StringWriter sw = new StringWriter();
							em.workspace().exportWorld(new PrintWriter(sw));
							world = sw.toString();
							exportRunning= false;
							sync.notifyAll();
						}
					}
					catch(IOException io)
					{
						
					}
				}
			});
		}
		
		public String getWorld()
		{
			synchronized( sync )
			{
				while( exportRunning || world.equals("") )
				{
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
	
	/**
	* Registers extension primitives.
	*/
	public void load(org.nlogo.api.PrimitiveManager manager)
	{
		manager.addPrimitive("node", new Node());
		manager.addPrimitive("node.connect", new NodeParams());
		manager.addPrimitive("acceptworkers", new AcceptWorkers());
		manager.addPrimitive("test", new Test());
		manager.addPrimitive("mapreduce", new MapReduce());
		
		manager.addPrimitive("values", new Values());
		
		manager.addPrimitive("config.input", new InputDir());
		manager.addPrimitive("config.output", new OutDir());
		manager.addPrimitive("config.mapper", new Mapper());
		manager.addPrimitive("config.reducer", new Reducer());
		
		manager.addPrimitive("__print.config", new PlotConfig());
	}
	
	/**
	* Initializes this extension.
	*/
	public void runOnce(org.nlogo.api.ExtensionManager em) throws org.nlogo.api.ExtensionException
	{
		Manager.em= (org.nlogo.workspace.ExtensionManager) em;
		Manager.world= new WorldSem();
		Manager.world.fillIn();
	}
	
	public static String getWorld()
	{
		return Manager.world.getWorld();
	}
}
