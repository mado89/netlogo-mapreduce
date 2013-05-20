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
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

public class MRHubNetMgr extends DefaultCommand {
	@Override
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	@Override
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		Framework fw= FrameworkFactory.getInstance();
		HubNetInterface hubnet = Manager.em.workspace().getHubNetManager();
		MapReduceRun mrrun= fw.getRun();
		if( mrrun.getClass() != MultiNodeRun.class)
			throw new ExtensionException("Needed a MultiNodeRun but got a " + mrrun.getClass());
		MultiNodeRun run= (MultiNodeRun) mrrun;
		boolean runIt= true;
		
		while(runIt)
        {
                System.out.println("Message loop");
                try {
                        if( hubnet.messageWaiting() )
                        {
                                hubnet.fetchMessage();
                                if( hubnet.enterMessage() )
                                {
                                	System.out.println("New Client");
                                    String name= "xx";
                                    run.newClient(name);
                                }
                                else if( hubnet.exitMessage() )
                                {
                                	System.out.println("Client left");
									String name = "xx";
									run.removeClient(name);
                                }
                                else
                                {
                                        
                                }
                        }
                        Thread.sleep(500);
                } catch (LogoException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
        }
	}
}