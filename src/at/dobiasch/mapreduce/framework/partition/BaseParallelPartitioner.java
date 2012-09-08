package at.dobiasch.mapreduce.framework.partition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;

public class BaseParallelPartitioner implements Callable<Object>
{
	private String path;
	private String checksum;
	
	protected int position;
	
	protected InputStream is;
	protected MessageDigest complete;
	
	private BufferedWriter out;
	
	public BaseParallelPartitioner(String path, String sysdir) throws NoSuchAlgorithmException, IOException
	{
		this.path= path;
		this.checksum= "";
		
		is= new FileInputStream(path);
		complete= MessageDigest.getInstance("SHA1");
		
		File file;
		file= new File(path);
		file= new File(sysdir + "/" + file.getName() + ".partition");
		out= new BufferedWriter(new FileWriter(file));
	}
	
	public Object call()
	{
		try
		{
			
			do
			{
				read();
				update();
				if( split() )
					writePartition();
			} while (doRead());
			is.close();
			
			byte[] b= complete.digest();
			
			checksum= "";
			for (int i=0; i < b.length; i++)
			{
				checksum += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String[] ret= new String[2];
		ret[0]= path;
		ret[1]= checksum;
		return ret;
	}
	
	protected Object read()
	{
		return null;
	}
	
	protected boolean doRead()
	{
		return false;
	}
	
	protected void update()
	{
		/*if( (Integer) tmp > 0)
		{
			complete.update(buffer, 0, numRead);
		}*/
	}

	protected boolean split()
	{
		return false;
	}
	
	protected void writePartition() throws IOException
	{
		out.write("" + position + "\n");
	}
}
