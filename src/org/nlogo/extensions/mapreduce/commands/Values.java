package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;
import org.nlogo.nvm.Workspace;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.controller.Data;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.inputparser.IInputParser;

public class Values extends DefaultReporter
{
	public Syntax getSyntax()
	{
		return Syntax.reporterSyntax( new int[] {  } , Syntax.ListType() ) ;
	}
	
	public Object report(Argument args[], Context context)
			throws ExtensionException, LogoException
	{
		Workspace ws= ((org.nlogo.nvm.ExtensionContext) context).workspace();
		Framework fw= FrameworkFactory.getInstance();
		HostController controller= fw.getTaskController();
		Data data= controller.getData(ws);
		
		// elem.ws.world.setObserverVariableByName("mapreduce.values", list.toLogoList());
		// System.out.println("Agt id " + context.getAgent().id());
		// context.getAgent().world();
		
		IInputParser inp= fw.newInputParser();
		return inp.parseInput(data);
	}
}
