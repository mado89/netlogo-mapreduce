package org.nlogo.extensions.mapreduce;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.nlogo.extensions.mapreduce.commands.AcceptWorkers;
import org.nlogo.extensions.mapreduce.commands.Emit;
import org.nlogo.extensions.mapreduce.commands.ID;
import org.nlogo.extensions.mapreduce.commands.Key;
import org.nlogo.extensions.mapreduce.commands.MRHubNetMgr;
import org.nlogo.extensions.mapreduce.commands.MapProgress;
import org.nlogo.extensions.mapreduce.commands.MapReduce;
import org.nlogo.extensions.mapreduce.commands.Node;
import org.nlogo.extensions.mapreduce.commands.NodeParams;
import org.nlogo.extensions.mapreduce.commands.Running;
import org.nlogo.extensions.mapreduce.commands.ParseInput;
import org.nlogo.extensions.mapreduce.commands.PlotConfig;
import org.nlogo.extensions.mapreduce.commands.ReduceProgress;
import org.nlogo.extensions.mapreduce.commands.Test;
import org.nlogo.extensions.mapreduce.commands.Values;
import org.nlogo.extensions.mapreduce.commands.config.InputDir;
import org.nlogo.extensions.mapreduce.commands.config.InputFormat;
import org.nlogo.extensions.mapreduce.commands.config.Mapper;
import org.nlogo.extensions.mapreduce.commands.config.OutDir;
import org.nlogo.extensions.mapreduce.commands.config.Reducer;
import org.nlogo.extensions.mapreduce.commands.config.ValueSeparator;

import at.dobiasch.mapreduce.framework.FrameworkFactory;

/**
 * Manager Class for the extension
 * @author Martin Dobiasch
 */
public class Manager extends org.nlogo.api.DefaultClassManager
{
	public static org.nlogo.workspace.ExtensionManager em;
	private static WorldSem world;
	// private static String key;
	// private static LogoList values;
	
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
		
		manager.addPrimitive("value", new Values());
		manager.addPrimitive("key", new Key());
		manager.addPrimitive("id", new ID());
		manager.addPrimitive("emit", new Emit());
		
		manager.addPrimitive("config.input", new InputDir());
		manager.addPrimitive("config.output", new OutDir());
		manager.addPrimitive("config.mapper", new Mapper());
		manager.addPrimitive("config.reducer", new Reducer());
		manager.addPrimitive("config.inputformat", new InputFormat());
		manager.addPrimitive("config.valueseparator", new ValueSeparator());
		
		manager.addPrimitive("running?", new Running());
		manager.addPrimitive("map-progress", new MapProgress());
		manager.addPrimitive("reduce-progress", new ReduceProgress());
		
		manager.addPrimitive("__print.config", new PlotConfig());
		
		manager.addPrimitive("__parseinput", new ParseInput());
		manager.addPrimitive("__mrhubnetmgr", new MRHubNetMgr());
	}
	
	/**
	* Initializes this extension.
	*/
	public void runOnce(org.nlogo.api.ExtensionManager em) throws org.nlogo.api.ExtensionException
	{
		Manager.em= (org.nlogo.workspace.ExtensionManager) em;
		Manager.world= new WorldSem();
		Manager.world.fillIn();
		
		FrameworkFactory.getInstance();
	}
	
	public static String getWorld()
	{
		return Manager.world.getWorld();
	}
	
	/*public static void setTaskData(String key, LogoList values)
	{
		Manager.key= key;
		Manager.values= values;
		System.out.println("set vals");
	}
	
	public static void cleanupTaskData()
	{
		Manager.key= null;
		Manager.values= null;
		System.out.println("clean up");
	}
	
	public static String getKey()
	{
		System.out.println("get key " + key);
		return Manager.key;
	}
	
	public static LogoList getValues()
	{
		System.out.println("get vals " + values);
		return Manager.values;
	}*/
}
