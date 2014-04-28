package at.dobiasch.mapreduce.framework;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.nlogo.api.ExtensionException;

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
	
	public SysFileHandler(String dirname, int jobnumber, String location) throws ExtensionException
	{
		/*File loc= new File(location);
		if( !loc.exists() )
			loc.mkdirs();
		
		File f;
		try {
			f = File.createTempFile(dirname, "" + jobnumber, loc);
			this.sysdir= f.getAbsolutePath();
			if(!(f.mkdir()))
		    {
		        throw new ExtensionException("Could not create temp directory: " + f.getAbsolutePath());
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ExtensionException("Cannot create temporary directory");
		}*/
		this.sysdir= location + dirname + "-" + jobnumber;
		File f= new File(sysdir);
		// File.createTempFile("", "", sysdir);
		this.sysdir= f.getAbsolutePath();
		if( !f.exists() )
			f.mkdirs();
		if( this.sysdir.endsWith("/") || this.sysdir.endsWith("\\"))
			this.sysdir= this.sysdir.substring(0, this.sysdir.length() - 1);
		System.out.println("Temporary directory " + this.sysdir + " created");
		this.files= new HashMap<String,String>();
	}
	
	/**
	 * Add a directory to the temporary files
	 * @param dir
	 */
	public String addDirectory(String dir)
	{
		String dirn= this.sysdir + File.separator + dir;
		File f= new File(dirn);
		if( !f.exists() )
			f.mkdirs();
		
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
			
			files.put(dir, dirn);
			
			syncFileswait= false;
			syncFiles.notifyAll();
		}
		
		return dirn;
	}
	
	/**
	 * Add a file to the file-management
	 * Returns a path to the file in the system. 
	 * @param fn
	 * @return a filename
	 */
	public String addFile(String fn)
	{
		String intfn= sysdir + File.separator + fn;
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
		System.out.println("SysFileHandler::cleanSysDir");
		for(String filename : files.values()) {
			File f= new File(filename);
			f.deleteOnExit();
		}
		File dir= new File(sysdir);
		dir.deleteOnExit();
	}

	public String getFile(String fn) {
		return sysdir + "/" + fn;
	}
}
