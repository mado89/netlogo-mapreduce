package org.nlogo.extensions.mapreduce.commands;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoListBuilder;
import org.nlogo.api.Syntax;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.LogoObject;
import at.dobiasch.mapreduce.framework.TaskType;
import at.dobiasch.mapreduce.framework.controller.Data;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.inputparser.IInputParser;

/**
 * 
 * @author Martin Dobiasch
 */
public class ParseInput extends DefaultCommand
{
	public Syntax getSyntax()
	{
		return Syntax.commandSyntax(new int[] {});
	}
	
	public void perform(Argument args[], Context context) throws ExtensionException
	{
		AbstractWorkspace ws= (AbstractWorkspace) ((org.nlogo.nvm.ExtensionContext) context).workspace();
		Framework fw= FrameworkFactory.getInstance();
		HostController controller= fw.getTaskController();
		Data data= controller.getData(ws);
		
		if( data.type == TaskType.Reduce )
		{
			try
			{
				RandomAccessFile in = new RandomAccessFile(data.src, "r");
				byte[] b= new byte[(int) (data.end - data.start)];
				
				in.seek(data.start);
				in.read(b);
				
				in.close();
				
				LogoListBuilder list = new LogoListBuilder();
				String[] vals= new String(b).split("\n");
				b= null;
				for(int i= 0; i < vals.length; i++)
					list.add(vals[i].replaceAll("\\r|\\n", ""));
				
				System.out.println("read " + data.ID + " " + data.key + " " + vals[0].replaceAll("\\n","") + " " + vals.length);
				
				LogoObject o= new LogoObject();
				o.set(list.toLogoList());
				data.value= o;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new ExtensionException(e);
			} catch (IOException e) {
				e.printStackTrace();
				throw new ExtensionException(e);
			}
		}
		else if( data.type == TaskType.Map )
		{
			IInputParser inp= fw.newInputParser();
			synchronized( inp)
			{
				inp.parseInput(data);
				
				data.key= inp.getKey();
				data.value= inp.getValues();
				// if( ! data.values.isEmpty() )
				// 	System.out.println(data.ID + " " + ws + " " + data + " " + data.values.first());
			}
		}
	}
}
