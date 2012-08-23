package org.nlogo.extensions.mapreduce.commands;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.HubNetInterface;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import scala.collection.Seq;

import at.dobiasch.mapreduce.framework.Framework;
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
		Framework fw= FrameworkFactory.getInstance();
		fw.getConfiguration().setInputDirectory("asdf");
		Map<String,String> fields= fw.getConfiguration().getChangedFields();
		
		try
		{
			HubNetInterface hubnet = Manager.em.workspace().getHubNetManager();
			
			// Create client set
			scala.collection.Seq<String> clients= hubnet.clients().toSeq();
			
			System.out.println(clients);
			System.out.println(fields);
			
			hubnet.send(clients, "CONFIG", fields);
		} catch (LogoException e) {
			throw new ExtensionException(e);
		}
	}
}
