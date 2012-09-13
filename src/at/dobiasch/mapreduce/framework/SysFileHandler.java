package at.dobiasch.mapreduce.framework;

import java.util.HashMap;
import java.util.Map;

/**
 * Book-Keeper of all System files
 * @author Martin Dobiasch
 *
 */
public class SysFileHandler
{
	private String sysdir;
	private Map<String,String> files;
	
	public SysFileHandler(String sysdir)
	{
		this.sysdir= sysdir;
		this.files= new HashMap<String,String>();
	}
	
	public String addFile(String fn)
	{
		files.put(fn, sysdir + "/" + fn);
		return sysdir + "/" + fn;
	}
	
	public void removeFile(String fn)
	{
		files.remove(fn);
	}
	
	public void cleanSysDir()
	{
		// TODO
	}
}
