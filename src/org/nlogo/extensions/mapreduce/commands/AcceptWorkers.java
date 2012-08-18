package org.nlogo.extensions.mapreduce.commands;

import java.util.ArrayList;
import java.util.List;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;

import org.nlogo.extensions.mapreduce.Manager;

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
		Manager.em.workspace().getHubNetManager().reset();
		scala.collection.Iterable<Object> list= new scala.collection.mutable.LinkedList<Object>();
		try
		{
			Manager.em.workspace().getHubNetManager().setClientInterface("MAPREDUCE", list);
		} 
		catch (LogoException e)
		{
			throw new ExtensionException(e);
		}
	}
}
