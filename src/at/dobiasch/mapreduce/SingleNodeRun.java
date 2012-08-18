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
	
	public SingleNodeRun(Framework fw)
	{
		this.fw= fw;
	}
	
	public void setup() throws ExtensionException
	{
		try
		{
			wb= new WorkspaceBuffer(size ,world, modelpath);
			wb.compileComands(fw.getConfiguration().getMapper(), fw.getConfiguration().getReducer());
		} catch (CompilerException e1)
		{
			throw new ExtensionException(e1);
		} catch (IOException e) {
			throw new ExtensionException(e);
		}
	}
}
