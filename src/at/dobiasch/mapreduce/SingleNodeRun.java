package at.dobiasch.mapreduce;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.partition.ICheckAndPartition;
import at.dobiasch.mapreduce.framework.partition.ParallelPartitioner;

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
			
			ICheckAndPartition part= new ParallelPartitioner();
			part.init("/tmp", 100);
			
			String indir= fw.getConfiguration().getInputDirectory();
			File inputdir = new File(indir);
			
			FilenameFilter filter = new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return !name.startsWith(".");
			    }
			};

			String[] children = inputdir.list(filter);
			if (children == null)
			{
			    // Either dir does not exist or is not a directory
			}
			else
			{
			    for (int i=0; i<children.length; i++) {
			        // Get filename of file or directory
			    	System.out.println(children[i]);
			        part.addFile(indir + "/" + children[i]);
			    }
			}
			
			Map<String,String> ret= part.getChecksums();
			System.out.println(ret);
		} catch (CompilerException e1){
			throw new ExtensionException(e1);
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (Exception e) {
			throw new ExtensionException(e);
		}
	}
}
