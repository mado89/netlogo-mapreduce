package at.dobiasch.mapreduce.framework;

import java.io.File;
import java.io.FilenameFilter;

import at.dobiasch.mapreduce.framework.partition.CheckPartData;
import at.dobiasch.mapreduce.framework.partition.ICheckAndPartition;

public class InputChecker
{
	private Framework fw;
	private CheckPartData indata;

	public InputChecker(Framework fw)
	{
		this.fw= fw;
	}
	
	public CheckPartData getData()
	{
		return indata;
	}
	
	public void check() throws Exception
	{
		ICheckAndPartition part= fw.getNewPartitioner();
		String indir= fw.getConfiguration().getInputDirectory();
		File inputdir = new File(indir);
		
		if( inputdir.isFile() )
		{
			if( inputdir.isAbsolute() )
				part.addFile(indir);
			else
				part.addFile(fw.getConfiguration().getBaseDir() + indir);
		}
		else
		{
			FilenameFilter filter = new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return !name.startsWith(".");
			    }
			};
	
			String[] children;
			if( !inputdir.isAbsolute() )
			{
				indir= fw.getConfiguration().getBaseDir() + indir;
				inputdir= new File(indir);
			}
			children= inputdir.list(filter);
			
			if (children == null)
			{
			    // Either dir does not exist or is not a directory
			}
			else
			{
			    for (int i=0; i<children.length; i++) {
			        // Get filename of file or directory
			    	// System.out.println(children[i]);
			        part.addFile(indir + "/" + children[i]);
			    }
			}
		}
		
		indata= part.getData();
		
		System.out.println(indata);
	}
}
