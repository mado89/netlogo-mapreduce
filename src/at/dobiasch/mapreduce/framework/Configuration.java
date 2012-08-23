package at.dobiasch.mapreduce.framework;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Configuration
{
	private String indir, outdir;
	private String mapper, reducer;
	private int mappers, reducers;
	
	private static final String INDIR    = "input";
	private static final String OUTDIR   = "output";
	private static final String MAPPER   = "mapper";
	private static final String REDUCER  = "reducer";
	private static final int    MAPPERS  = Runtime.getRuntime().availableProcessors() * 100;
	private static final int    REDUCERS = Runtime.getRuntime().availableProcessors() * 10;
	
	public Configuration()
	{
		indir= INDIR;
		outdir= OUTDIR;
		mapper= MAPPER;
		reducer= REDUCER;
		mappers= MAPPERS;
		reducers= REDUCERS;
	}
	
	public String getInputDirectory()
	{
		return indir;
	}
	
	/**
	 * Sets the location of the directory of the input for the job
	 * @param indir The value to assign indir.
	 */
	public void setInputDirectory(String indir)
	{
		this.indir = indir;
	}
	
	/**
	 * Returns the value of outdir.
	 */
	public String getOutputDirectory()
	{
		return outdir;
	}
	
	/**
	 * Sets the location where the output of the job will be stored.
	 * @param outdir The value to assign outdir.
	 */
	public void setOutputDirectory(String outdir)
	{
		this.outdir = outdir;
	}

	public String getMapper()
	{
		return this.mapper;
	}

	public String getReducer()
	{
		return this.reducer;
	}

	/**
	 * Returns a map with changed values. 
	 * This map can be used to for setValues
	 * @see setValues
	 * @return a map
	 */
	public Map<String,String> getChangedFields()
	{
		Map<String,String> map= new HashMap<String,String>();
		
		if(!this.indir.equals(INDIR))
			map.put("INDIR", indir);
		if(!this.outdir.equals(OUTDIR))
			map.put("OUTDIR", indir);
		if(!this.mapper.equals(MAPPER))
			map.put("MAPPER", indir);
		if(!this.reducer.equals(REDUCER))
			map.put("REDUCER", indir);
		if( this.mappers != MAPPERS )
			map.put("MAPPERS", "" + this.mappers);
		if( this.reducers != REDUCERS )
			map.put("REDUCERS", "" + this.reducers);
		
		return map;
	}
	
	/**
	 * All values in the Map fields. Values not included in the map won't be included in any changes
	 * @param fields
	 */
	public void setValues(Map<String,String> fields)
	{
		Set<String> keys= fields.keySet();
		Collection<String> vals= fields.values();
		Iterator<String> i = vals.iterator();
		for(String key : keys)
		{
			if( key.equals("INDIR"))
				this.indir= "" + i;
			if( key.equals("OUTDIR"))
				this.outdir= "" + i;
			if( key.equals("MAPPER"))
				this.mapper= "" + i;
			if( key.equals("REDUCER"))
				this.reducer= "" + i;
			if( key.equals("MAPPERS"))
				this.mappers= Integer.parseInt("" + i);
			if( key.equals("REDUCERS"))
				this.reducers= Integer.parseInt("" + i);
			i.next();
		}
	}
	
	public String toString()
	{
		String ret;
		ret= "Config [";
		ret+= "indir: " + this.indir;
		ret+= ",outdir: " + this.outdir;
		ret+= ",mapper: " + this.mapper;
		ret+= ",reducer: " + this.reducer;
		ret+= ",mappers: " + this.mappers;
		ret+= ",reducers: " + this.reducers;
		ret+= "]";
		return ret;
	}
}
