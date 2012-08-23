package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

/**
 * 
 * @author Martin Dobiasch
 */
public class Test extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		System.out.println("TEST");
		
		try
		{
			HubNetInterface hubnet = Manager.em.workspace().getHubNetManager();
			FrameworkFactory.getInstance().sendConfigToClients(hubnet);
		} catch (FrameworkException e) {
			throw new ExtensionException(e);
		}
	}
}
