package org.nlogo.extensions.mapreduce.commands;

import java.util.Iterator;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import scala.collection.Iterable;

import at.dobiasch.mapreduce.MapReduceRun;
import at.dobiasch.mapreduce.MultiNodeRun;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.multi.MapRedHubNetManager;

public class MRHubNetMgr extends DefaultCommand {
	HubNetInterface hubnet;
	MultiNodeRun run;
	Framework fw;
	
	@Override
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	@Override
	public void perform(Argument args[], Context context)
			throws ExtensionException {
		
		fw = FrameworkFactory.getInstance();
		hubnet = Manager.em.workspace().getHubNetManager();
		/* MapReduceRun mrrun = fw.getRun();
		if (mrrun.getClass() != MultiNodeRun.class)
			throw new ExtensionException("Needed a MultiNodeRun but got a "
					+ mrrun.getClass());
		MultiNodeRun run = (MultiNodeRun) mrrun;*/
		boolean runIt = true;

		while (runIt) {
			System.out.println("Message loop");
			try {
				if (hubnet.messageWaiting()) {
					hubnet.fetchMessage();
					if (hubnet.enterMessage()) {
						System.out.println("New Client");
						((MultiNodeRun) fw.getRun()).newClient("node");
						fw.getHubNetManager().doneFeeding();
						// run.newClient(name);
					} else if (hubnet.exitMessage()) {
						System.out.println("Client left");
						String name = "xx";
						// run.removeClient(name);
					} else {

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
	
	// @Override
	public void performNew(Argument args[], Context context) throws ExtensionException
	{
		fw= FrameworkFactory.getInstance();
		hubnet= Manager.em.workspace().getHubNetManager();
		int oldstate= fw.getHubNetManager().getState();
		int state= fw.getHubNetManager().getState();
		// TODO: tricky: run vlt ja noch garnicht gestartet
		/*
		 * Ist im Prinzip einfach: HubNetManager bekommt ein Signal wenn der Run da ist
		 * dann wechselt er den State
		 * hier brauch ich eine StateMachine (um mehrere runs hintereinander haben zu koennen)
		 * Im 1. State werden nodes gesammelt
		 * 1 -> 2 durch HubNetManager
		 * 2. State: fuetter dem Run die Nodes (achtung node kann schon wieder weg sein)
		 * 3. State: Lauschen auf neue nodes / nodes verlassen (run mitteilen)
		 * 4. State: nodes fuer State 1 vorbereiten (run ist vorbei)
		 */
		boolean runIt= true;
		
		while(runIt)
        {
			state= fw.getHubNetManager().getState();
			System.out.println("Message loop " + state);
			
			switch( state )
			{
				case MapRedHubNetManager.STATE_INIT: // do nothing
					idleFetch();
					break;
				case MapRedHubNetManager.STATE_RUNSTARTED:
					prepareRun();
					break;
				case MapRedHubNetManager.STATE_RUN:
					feedNodes();
					break;
				case MapRedHubNetManager.STATE_RUNFINISHED:
					// TODO: what to do here?
					break;
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
                e.printStackTrace();
			}
        }
	}
	
	private void idleFetch() throws ExtensionException {
		try {
            if( hubnet.messageWaiting() )
            {
                    hubnet.fetchMessage();
                    String name= hubnet.getMessageSource();
                	
                    if( hubnet.enterMessage() )
                    {
                    	System.out.println("New Client " + name);
                    }
                    else if( hubnet.exitMessage() )
                    {
                    	System.out.println("Client left " + name);
                    }
                    else
                    {
                    	System.out.println("other message");
                    }
            }
            else
            	System.out.println("No msg");
    } catch (LogoException e) {
            e.printStackTrace();
            throw new ExtensionException(e);
    }
	}

	private void prepareRun() throws ExtensionException
	{
		MapReduceRun mrrun= fw.getRun();
		if( mrrun.getClass() != MultiNodeRun.class)
			throw new ExtensionException("Needed a MultiNodeRun but got a " + mrrun.getClass());
		run= (MultiNodeRun) mrrun;
		
		scala.collection.Iterator<String> it= hubnet.clients().iterator();
		String node;
		
		System.out.println("Feeding nodes");
		
		while(it.hasNext())
		{
			node= it.next();
			run.newClient(node);
		}
		
		fw.getHubNetManager().doneFeeding();
	}

	private void feedNodes() throws ExtensionException
	{
        try {
                if( hubnet.messageWaiting() )
                {
                        hubnet.fetchMessage();
                        if( hubnet.enterMessage() )
                        {
                        	System.out.println("New Client");
                            String name= hubnet.getMessageSource();
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
        } catch (LogoException e) {
                e.printStackTrace();
                throw new ExtensionException(e);
        }
	}
}
