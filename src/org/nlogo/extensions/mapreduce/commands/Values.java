package org.nlogo.extensions.mapreduce.commands;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.Syntax;


public class Values extends DefaultReporter
{
	public Syntax getSyntax()
	{
		return Syntax.reporterSyntax( new int[] {  } , Syntax.ListType() ) ;
	}
	
	public Object report(Argument args[], Context context)
			throws ExtensionException, LogoException
	{
		// elem.ws.world.setObserverVariableByName("mapreduce.values", list.toLogoList());
		System.out.println("Agt id " + context.getAgent().id());
		// context.getAgent().world();
		return null;
	}
}