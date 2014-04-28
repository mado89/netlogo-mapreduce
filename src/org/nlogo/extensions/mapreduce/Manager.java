package org.nlogo.extensions.mapreduce;

import java.io.File;

import org.nlogo.api.ExtensionException;
import org.nlogo.api.ExtensionManager;
import org.nlogo.extensions.mapreduce.commands.AcceptWorkers;
import org.nlogo.extensions.mapreduce.commands.Emit;
import org.nlogo.extensions.mapreduce.commands.ID;
import org.nlogo.extensions.mapreduce.commands.Key;
import org.nlogo.extensions.mapreduce.commands.MRHubNetMgr;
import org.nlogo.extensions.mapreduce.commands.MapProgress;
import org.nlogo.extensions.mapreduce.commands.MapReduce;
import org.nlogo.extensions.mapreduce.commands.Node;
import org.nlogo.extensions.mapreduce.commands.NodeParams;
import org.nlogo.extensions.mapreduce.commands.Result;
import org.nlogo.extensions.mapreduce.commands.Running;
import org.nlogo.extensions.mapreduce.commands.ParseInput;
import org.nlogo.extensions.mapreduce.commands.PlotConfig;
import org.nlogo.extensions.mapreduce.commands.ReduceProgress;
import org.nlogo.extensions.mapreduce.commands.Values;
import org.nlogo.extensions.mapreduce.commands.config.InputDir;
import org.nlogo.extensions.mapreduce.commands.config.InputFormat;
import org.nlogo.extensions.mapreduce.commands.config.Mapper;
import org.nlogo.extensions.mapreduce.commands.config.OutDir;
import org.nlogo.extensions.mapreduce.commands.config.Reducer;
import org.nlogo.extensions.mapreduce.commands.config.ValueSeparator;
import org.nlogo.nvm.Workspace;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.WorldSem;

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
	* Registers extension primitives.
	*/
	@Override
	public void load(org.nlogo.api.PrimitiveManager manager)
	{
		manager.addPrimitive("node", new Node());
		manager.addPrimitive("node.connect", new NodeParams());
		manager.addPrimitive("acceptworkers", new AcceptWorkers());
		manager.addPrimitive("mapreduce", new MapReduce());
		manager.addPrimitive("result", new Result());
		
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
	@Override
	public void runOnce(org.nlogo.api.ExtensionManager em) throws org.nlogo.api.ExtensionException
	{
		Manager.em= (org.nlogo.workspace.ExtensionManager) em;
		Manager.world= new WorldSem();
		// Manager.world.fillIn();
		
		/**
		 * Setup the Framework
		 */
		Framework fw= FrameworkFactory.getInstance();
		
		// Determine where the model is
		File model= new File(Manager.em.workspace().getModelPath());
		String absolutePath = model.getAbsolutePath();
		String modelPath = absolutePath.substring(0,absolutePath.lastIndexOf(File.separator));
		fw.getConfiguration().setBaseDir(modelPath + File.separator);
		// Set the worldsempahore
		fw.setWorldSem(Manager.world);
	}
	
	/**
	 * When NetLogo is closing properly --> clean up
	 * @throws ExtensionException in case the Framework can not be loaded
	 */
	@Override
	public void unload(ExtensionManager em) throws ExtensionException 
	{
		Framework fw= FrameworkFactory.getInstance();
		fw.cleanup();
		System.out.println("Manager::unload");
	}
	
	/**
	 * Request the current(!) world
	 * @return
	 */
	/*public static String requestWorld()
	{
		Manager.world.fillIn();
		return Manager.world.getWorld();
	}*/
	
	@Deprecated
	public static String getWorld()
	{
		return Manager.world.getWorld();
	}

	public static String requestWorld(Workspace workspace) {
		Manager.world.fillIn((AbstractWorkspace) workspace);
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
