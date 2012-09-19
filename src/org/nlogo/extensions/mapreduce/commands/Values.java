package org.nlogo.extensions.mapreduce.commands;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoListBuilder;
import org.nlogo.api.Syntax;
import org.nlogo.nvm.Workspace;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.controller.Data;
import at.dobiasch.mapreduce.framework.controller.HostController;

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
		HostController controller= FrameworkFactory.getInstance().getTaskController();
		Data data= controller.getData(ws);
		
		// elem.ws.world.setObserverVariableByName("mapreduce.values", list.toLogoList());
		// System.out.println("Agt id " + context.getAgent().id());
		// context.getAgent().world();
		
		RandomAccessFile in;
		try
		{
			in = new RandomAccessFile(data.src, "r");
			byte[] b= new byte[(int) (data.end - data.start)];
			
			in.seek(data.start);
			in.read(b);
			
			in.close();
			
			LogoListBuilder list = new LogoListBuilder();
			String[] vals= new String(b).split("\n");
			b= null;
			for(int i= 0; i < vals.length; i++)
				list.add(vals[i].replaceAll("\\r|\\n", ""));
			
			// System.out.println("running " + data.ID + " " + data.start + " " + data.end + " " + vals.length);
			
			return list.toLogoList();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}
}
