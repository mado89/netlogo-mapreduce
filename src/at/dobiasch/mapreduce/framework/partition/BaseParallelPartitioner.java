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

import at.dobiasch.mapreduce.framework.partition.ICheckAndPartition.CheckPartData;

public class BaseParallelPartitioner implements Callable<Object>
{
	private String path;
	private String checksum;
	
	protected int position;
	protected long filesize;
	protected int blocksize;
	protected int partitions;
	protected String partfn;
	
	protected InputStream is;
	protected MessageDigest complete;
	
	protected BufferedWriter out;
	
	public BaseParallelPartitioner(String path, String sysdir, Integer blocksize)
			throws NoSuchAlgorithmException, IOException
	{
		this.path= path;
		this.checksum= "";
		this.blocksize= blocksize;
		this.partitions= 0;
		
		is= new FileInputStream(path);
		complete= MessageDigest.getInstance("SHA1");
		
		File file;
		file= new File(path);
		filesize= file.length();
		this.partfn= sysdir + "/" + file.getName() + ".partition";
		file= new File(partfn);
		out= new BufferedWriter(new FileWriter(file));
	}
	
	public Object call()
	{
		try
		{
			do
			{
				read();
				if( split() )
					writePartition();
				update();
			} while (doRead());
			is.close();
			
			byte[] b= complete.digest();
			
			checksum= convToHex(b);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		CheckPartData ret= new CheckPartData();
		// String[] ret= new String[2];
		ret.key= path;
		ret.checksum= checksum;
		ret.partitionfile= this.partfn;
		ret.numpartitions= this.partitions + 1;
		
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ret;
	}
	
	private static String convToHex(byte[] data)
	{
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int halfbyte = (data[i] >>> 4) & 0x0F;
            int two_halfs = 0;
            do {
                if ((0 <= halfbyte) && (halfbyte <= 9))
                    buf.append((char) ('0' + halfbyte));
                else
                	buf.append((char) ('a' + (halfbyte - 10)));
                halfbyte = data[i] & 0x0F;
            } while(two_halfs++ < 1);
        }
        return buf.toString();
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
		this.partitions ++ ;
	}
}
