package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.MapReduceRun;
import at.dobiasch.mapreduce.SingleNodeRun;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

/**
 * 
 * @author Martin Dobiasch
 */
public class MapReduce extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		HubNetInterface hubnet = Manager.em.workspace().getHubNetManager();
		Framework fw = FrameworkFactory.getInstance();
		if( fw.getNNodes(hubnet) == 0 )
		{
			String world,model;
			world= Manager.getWorld();
			model= Manager.em.workspace().getModelPath();
			MapReduceRun run;
			if( fw.isMultiNode() )
				run= new SingleNodeRun(fw,world,model);
			else
				run= new SingleNodeRun(fw,world,model);
			fw.setRun(run);
			run.setup();
			run.startRun();
		}
	}
}
