package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.MapReduceRun;
import at.dobiasch.mapreduce.MultiNodeRun;
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
		return Syntax.commandSyntax(new int[] {
				Syntax.StringType() /*Mapper*/, 
				Syntax.StringType() /*Reducer*/,
				Syntax.WildcardType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		HubNetInterface hubnet = Manager.em.workspace().getHubNetManager();
		Framework fw = FrameworkFactory.getInstance();
		
		try {
			fw.getConfiguration().setMapper(args[0].getString());
			fw.getConfiguration().setReducer(args[1].getString());
			fw.getConfiguration().setAccumulatorFromObj(args[2].get());
		} catch (LogoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String world,model;
		world= Manager.getWorld();
		model= Manager.em.workspace().getModelPath();
		MapReduceRun run;
		if( fw.isMultiNode() )
			run= new MultiNodeRun(fw,world,model);
		else
			run= new SingleNodeRun(fw,world,model);
		fw.setRun(run);
		run.setup();
		run.startRun();
	}
}
