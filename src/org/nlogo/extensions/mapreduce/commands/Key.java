package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.controller.Data;
import at.dobiasch.mapreduce.framework.controller.HostController;

public class Key extends DefaultReporter
{
	public Syntax getSyntax()
	{
		return Syntax.reporterSyntax( new int[] {  } , Syntax.StringType() ) ;
	}
	
	public Object report(Argument args[], Context context)
			throws ExtensionException, LogoException
	{
		AbstractWorkspace ws= (AbstractWorkspace) ((org.nlogo.nvm.ExtensionContext) context).workspace();
		HostController controller= FrameworkFactory.getInstance().getTaskController();
		Data data= controller.getData(ws);
		
		return data.key;
		// return Manager.getKey();
	}
}
