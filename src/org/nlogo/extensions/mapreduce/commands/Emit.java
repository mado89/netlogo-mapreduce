package org.nlogo.extensions.mapreduce.commands;

import java.io.IOException;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.controller.HostController;

public class Emit extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {Syntax.StringType(), Syntax.StringType()});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		String key= "";
		String value= "";
		
		try
		{
			key = args[0].getString();
			value = args[1].getString();
	    }
	    catch(LogoException e)
	    {
	      throw new ExtensionException( e.getMessage() ) ;
	    } 
		
		AbstractWorkspace ws= (AbstractWorkspace) ((org.nlogo.nvm.ExtensionContext) context).workspace();
		HostController controller= FrameworkFactory.getInstance().getTaskController();
		try
		{
			controller.emit(ws, key, value);
		} catch (IOException e) {
			throw new ExtensionException(e);
		}
	}
}

