package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.SingleNodeRun;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

/**
 * 
 * @author Martin Dobiasch
 */
public class SingleNode extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		Framework framework= FrameworkFactory.getInstance();
		
		Manager.requestWorld(((org.nlogo.nvm.ExtensionContext) context).workspace());
		SingleNodeRun run= new SingleNodeRun(framework,Manager.em.workspace().getModelPath());
		run.setup();
	}
}
