package at.dobiasch.mapreduce;

import java.io.IOException;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;

public class SingleNodeRun
{
	String world;
	String modelpath;
	int size;
	WorkspaceBuffer wb;
	Framework fw;
	
	public SingleNodeRun(Framework fw, String world, String modelpath)
	{
		this.fw= fw;
		this.world= world;
		this.modelpath= modelpath;
	}
	
	public void setup() throws ExtensionException
	{
		System.out.println("Setting up");
		try
		{
			this.size= fw.getConfiguration().getMappers();
			wb= new WorkspaceBuffer(size ,world, modelpath);
			wb.compileComands(fw.getConfiguration().getMapper(), fw.getConfiguration().getReducer());
		} catch (CompilerException e1){
			throw new ExtensionException(e1);
		} catch (IOException e) {
			throw new ExtensionException(e);
		}
	}
}
