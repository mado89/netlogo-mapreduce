package org.nlogo.extensions.mapreduce.commands.config;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;

import at.dobiasch.mapreduce.framework.FrameworkFactory;

public class ValueSeparator extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.StringType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String val= "";
		
		try
		{
	      val = args[0].getString();  
	    }
	    catch(LogoException e)
	    {
	      throw new ExtensionException( e.getMessage() ) ;
	    }
	    
		FrameworkFactory.getInstance().getConfiguration().setValueSeperator(val);
	}
}
