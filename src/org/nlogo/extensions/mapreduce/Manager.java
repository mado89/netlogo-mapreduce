package org.nlogo.extensions.mapreduce;

import org.nlogo.extensions.mapreduce.commands.AcceptWorkers;
import org.nlogo.extensions.mapreduce.commands.Node;

/**
 * Manager Class for the extension
 * @author Martin Dobiasch
 */
public class Manager extends org.nlogo.api.DefaultClassManager
{
	public static org.nlogo.workspace.ExtensionManager em;
	
	/**
	* Registers extension primitives.
	*/
	public void load(org.nlogo.api.PrimitiveManager manager)
	{
		manager.addPrimitive("node", new Node());
		manager.addPrimitive("acceptworkers", new AcceptWorkers());
	}
	
	/**
	* Initializes this extension.
	*/
	public void runOnce(org.nlogo.api.ExtensionManager em) throws org.nlogo.api.ExtensionException
	{
		Manager.em= (org.nlogo.workspace.ExtensionManager) em;
	}
}
