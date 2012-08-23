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
		Framework fw= FrameworkFactory.getInstance();
		fw.getConfiguration().setInputDirectory("asdf");
		Map<String,String> fields= fw.getConfiguration().getChangedFields();
		
		try
		{
			HubNetInterface hubnet = Manager.em.workspace().getHubNetManager();
			
			// Create client set
			scala.collection.Seq<String> clients= hubnet.clients().toSeq();
			
			// Initiate Transfer of configuration
			Manager.em.workspace().getHubNetManager().broadcast("CONFIG-START");
			
			Set<String> keys= fields.keySet();
			Collection<String> vals= fields.values();
			Iterator<String> i = vals.iterator();
			
			for(String key : keys)
			{
				// hubnet.send(clients, key, i);
				// hubnet.broadcast(key,i);
				i.next();
			}
			
			// End Transfer of configuration
			Manager.em.workspace().getHubNetManager().broadcast("CONFIG-END");
		} catch (LogoException e) {
			throw new ExtensionException(e);
		}
	}
}
