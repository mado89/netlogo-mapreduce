package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

/**
 * 
 * @author Martin Dobiasch
 */
public class Node extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		
		String world,model;
		world= Manager.getWorld();
		model= Manager.em.workspace().getModelPath();
		
		new at.dobiasch.mapreduce.Node(world, model);
	}
}
