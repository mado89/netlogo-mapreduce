package org.nlogo.extensions.mapreduce.commands;

import java.util.ArrayList;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import scala.collection.immutable.Stack;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

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
		// scala.collection.Iterable<Object> list= new scala.collection.mutable.LinkedList<Object>();
		ArrayList<Object> list= new ArrayList<Object>();
		list.add("CONFIG");
		try
		{
			// Manager.em.workspace().getHubNetManager().setClientInterface("MAPREDUCE", list);
			Manager.em.workspace().getHubNetManager().setClientInterface("MAPREDUCE", 
				scala.collection.JavaConversions.collectionAsScalaIterable(list));
		}
		catch (LogoException e)
		{
			throw new ExtensionException(e);
		}
	}
	
	public void perform2(Argument args[], Context context) throws ExtensionException
	{
				// scala.collection.Iterable<Object> list= new scala.collection.mutable.LinkedList<Object>();
				// Stack<Object> stack= new Stack<Object>();
				// stack.push("CONFIG");
				ArrayList<Object> list= new ArrayList<Object>();
				list.add("CONFIG");
//				try
//				{
					//Manager.em.workspace().getHubNetManager().setClientInterface("MAPREDUCE", stack);
					/*Manager.em.workspace().getHubNetManager().setClientInterface("MAPREDUCE", 
						scala.collection.JavaConversions.collectionAsScalaIterable(list));*/
					Manager.em.workspace().getHubNetManager().reset();
					FrameworkFactory.getInstance().setMaster(true);
//				} 
//				catch (LogoException e)
//				{
//					throw new ExtensionException(e);
//				}
	}
}
