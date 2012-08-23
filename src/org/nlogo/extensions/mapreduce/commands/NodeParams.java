package org.nlogo.extensions.mapreduce.commands;

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
public class NodeParams extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.StringType(),Syntax.NumberType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String name= "node"; // + Math.random();
		String host;
		int port;
		
		try {
			host= args[0].getString();
			port= args[1].getIntValue();
		} catch (LogoException e) {
			throw new ExtensionException(e);
		}
		
		
		new at.dobiasch.mapreduce.Node(name,host,port);
	}
}
