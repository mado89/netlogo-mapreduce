package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import at.dobiasch.mapreduce.framework.task.IntKeyVal;

public class MapOutWriter extends Thread {
	
	private RecordWriterBuffer mapout;
	private Map<String,IntKeyVal> intdata;
	private SysFileHandler sysfileh;
	private boolean running;
	private boolean finish;
	
	public MapOutWriter(RecordWriterBuffer mapout, Map<String,IntKeyVal> intdata,
			SysFileHandler sysfileh)
	{
		this.mapout= mapout;
		this.intdata= intdata;
		this.sysfileh= sysfileh;
		running= true;
		
		finish= false;
	}
	
	public void setFinishUp()
	{
		finish= true;
	}
	
	public void run()
	{
		while(running)
		{
			System.out.println("MapOutWriter run: " + this.mapout.getSize());
			try {
				RecordWriter w= this.mapout.get();
				RecordReader r= new RecordReader(w);
				System.out.println("MapOutWriter: " + w.getFilename());
				while(r.hasRecordsLeft())
				{
					String[] rec= r.readRecord();
					
					IntKeyVal h;
					String key;
					
					key= rec[0];
					if( key == null )
						key= "";
	                h= intdata.get(key);
	                // System.out.println(w.getFilename());
	                // System.out.println(r.recs);
	                // System.out.println(rec[0] + " " + rec[1]);
	                
	                if( h == null )
	                {
						String fn;
	                    MessageDigest md= null;
	                    try {
	                            md = MessageDigest.getInstance("SHA1");
	                    } catch (NoSuchAlgorithmException e) {
	                            e.printStackTrace();
	                    }
	                    md.update(key.getBytes());
	                    fn= sysfileh.addFile(ChecksumHelper.convToHex(md.digest()) + ".int");
	                    // System.out.println("New Key: " + key + fn);
	                    h= new IntKeyVal(key,fn);
	                    intdata.put(key, h);
	                }
	                
	                // System.out.println("Write " + key + " to " + h.fn);
	                h.writeValue(rec[1]);
				}
				if( !finish )
				{
					this.mapout.put(new RecordWriter(r));
					// Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			checkRun();
		}
	}
	
	/**
	 * Decide whether the deamon should continue to run or not
	 */
	private void checkRun()
	{
		if( finish )
			running= this.mapout.hasFiles();
	}

}
