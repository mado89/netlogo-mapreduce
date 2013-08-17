package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.CompilerException;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.multi.MapRedHubNetManager;

/**
 * 
 * @author Martin Dobiasch
 */
public class AcceptWorkers extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String model;
		
		try
		{
			System.out.println("Accept Workers");
			model= Manager.em.workspace().getModelPath();
			
			MapRedHubNetManager manager= new MapRedHubNetManager(model);
			
			FrameworkFactory.getInstance().setMultiNode(true);
			FrameworkFactory.getInstance().setHubNetManager(manager);
			
			manager.start();
		}
		catch (LogoException e)
		{
			throw new ExtensionException(e);
		} catch (CompilerException e) {
			throw new ExtensionException(e);
		}
		FrameworkFactory.getInstance().setMaster(true);
	}
}
