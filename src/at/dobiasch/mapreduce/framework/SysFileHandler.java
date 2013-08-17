package at.dobiasch.mapreduce.framework;

import java.io.File;
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
	private Object syncFiles= new Object();
	private boolean syncFileswait= false;
	
	public SysFileHandler(String sysdir)
	{
		this.sysdir= sysdir;
		File f= new File(sysdir);
		if( !f.exists() )
			f.mkdirs();
		if( this.sysdir.endsWith("/") )
			this.sysdir= this.sysdir.substring(0, this.sysdir.length() - 1);
		this.files= new HashMap<String,String>();
	}
	
	/**
	 * Add a file to the filemanagement
	 * @param fn
	 * @return a filename
	 */
	public String addFile(String fn)
	{
		String intfn= sysdir + "/" + fn;
		synchronized( syncFiles ) // get Intermediate-Data access for the key 
		{
			while( syncFileswait )
			{
				try
				{
					syncFiles.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			syncFileswait= true;
			
			files.put(fn, intfn);
			
			syncFileswait= false;
			syncFiles.notifyAll();
		}
		
		return intfn;
	}
	
	public void removeFile(String fn)
	{
		synchronized( syncFiles ) // get Intermediate-Data access for the key 
		{
			while( syncFileswait )
			{
				try
				{
					syncFiles.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			syncFileswait= true;
			
			files.remove(fn);
			
			syncFileswait= false;
			syncFiles.notifyAll();
		}
	}
	
	public void cleanSysDir()
	{
		// TODO
		System.out.println("SysFileHandler::cleanSysDir");
	}

	public String getFile(String fn) {
		return sysdir + "/" + fn;
	}
}
