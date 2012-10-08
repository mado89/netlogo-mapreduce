package org.nlogo.extensions.mapreduce.commands.config;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;

import at.dobiasch.mapreduce.framework.FrameworkFactory;

public class InputFormat extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.StringType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String val= "";
		String parserc;
		
		try
		{
	      val = args[0].getString();  
	    }
	    catch(LogoException e)
	    {
	      throw new ExtensionException( e.getMessage() ) ;
	    }
	    
		if( val.startsWith("_") )
		{
			parserc= val.substring(1);
			FrameworkFactory.getInstance().getConfiguration().setInputParser(parserc);
		}
		else if( val.equals("TextInput"))
		{
			parserc= "at.dobiasch.mapreduce.framework.inputparser.TextInputFormat";
			FrameworkFactory.getInstance().getConfiguration().setValueSeperator("\n");
			FrameworkFactory.getInstance().getConfiguration().setInputParser(parserc);
		}
		else if( val.equals("KeyValue"))
		{
			parserc= "at.dobiasch.mapreduce.framework.inputparser.KeyValueInputFormat";
			FrameworkFactory.getInstance().getConfiguration().setValueSeperator("\t");
			FrameworkFactory.getInstance().getConfiguration().setInputParser(parserc);
		}
		else
			throw new ExtensionException("Don't know input format " + val);
	}
}
