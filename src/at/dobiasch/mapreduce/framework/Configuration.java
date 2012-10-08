package at.dobiasch.mapreduce.framework;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.inputparser.IInputParser;

public class Configuration
{
	private String indir, outdir;
	private String mapper, reducer;
	private int mappers, reducers;
	private String parser;
	private String valsep;
	private IInputParser inp;
	
	private static final String INDIR    = "input";
	private static final String OUTDIR   = "output";
	private static final String MAPPER   = "mapper";
	private static final String REDUCER  = "reducer";
	private static final int    MAPPERS  = Runtime.getRuntime().availableProcessors() * 2;
	private static final int    REDUCERS = 1;
	private static final String PARSER   = "at.dobiasch.mapreduce.framework.inputparser.TextInputFormat";
	private static final String VALSEP  = "\n";
	
	public Configuration() throws ExtensionException
	{
		indir= INDIR;
		outdir= OUTDIR;
		mapper= MAPPER;
		reducer= REDUCER;
		mappers= MAPPERS;
		reducers= REDUCERS;
		valsep= VALSEP;
		this.setInputParser(PARSER);
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

	public int getMappers()
	{
		return mappers;
	}

	public void setMappers(int mappers)
	{
		this.mappers = mappers;
	}

	public int getReducers()
	{
		return reducers;
	}

	public void setReducers(int reducers)
	{
		this.reducers = reducers;
	}
	
	public String getMapper()
	{
		return mapper;
	}

	public void setMapper(String mapper)
	{
		this.mapper = mapper;
	}
	
	public String getReducer()
	{
		return reducer;
	}

	public void setReducer(String reducer)
	{
		this.reducer = reducer;
	}

	public String getValueSeperator()
	{
		return valsep;
	}
	
	public void setValueSeperator(String seperator)
	{
		this.valsep= seperator;
		this.inp.init(this.valsep);
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
		if( this.parser != PARSER )
			map.put("PARSER", "" + this.parser);
		if( this.valsep != VALSEP )
			map.put("VALSEP", this.valsep);
		
		return map;
	}
	
	/**
	 * All values in the Map fields. Values not included in the map won't be included in any changes
	 * @param fields
	 * @throws ExtensionException 
	 */
	public void setValues(Map<String,String> fields) throws ExtensionException
	{
		Set<String> keys= fields.keySet();
		Collection<String> vals= fields.values();
		Iterator<String> i = vals.iterator();
		for(String key : keys)
		{
			this.setValue(key, i.next());
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
		ret+= ",parser: " + this.parser;
		ret+= "]";
		return ret;
	}

	public void setValuesFromString(String valstring) throws ExtensionException
	{
		String[] vals= valstring.split(",");
		int i;
		for(i= 0; i < vals.length - 1; i++)
		{
			System.out.println(vals[i]);
			String[] h= vals[i].split("=");
			this.setValue(h[0], h[1]);
		}
	}
	
	public void setValue(String key, String value) throws ExtensionException
	{
		if( key.equals("INDIR"))
			this.indir= "" + value;
		if( key.equals("OUTDIR"))
			this.outdir= "" + value;
		if( key.equals("MAPPER"))
			this.mapper= "" + value;
		if( key.equals("REDUCER"))
			this.reducer= "" + value;
		if( key.equals("MAPPERS"))
			this.mappers= Integer.parseInt("" + value);
		if( key.equals("REDUCERS"))
			this.reducers= Integer.parseInt("" + value);
		if( key.equals("PARSER"))
			this.setInputParser(value);
		if( key.equals("VALSEP"))
			this.setValueSeperator(value);
	}
	
	
	public void setInputParser(String classname) throws ExtensionException
	{
		this.parser= classname;
		@SuppressWarnings("rawtypes") //TODO: maybe make this better
		Class clazz;
		try {
			clazz = Class.forName(this.parser);
			this.inp= (IInputParser) clazz.newInstance();
			this.inp.init(this.valsep);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new ExtensionException("Can't load" + this.parser + " (ClassNotFound)");
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new ExtensionException("Can't load" + this.parser + " (InstantiationException)");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new ExtensionException("Can't load" + this.parser + " (IllegalAccessException)");
		}
	}

	public IInputParser getParser()
	{
		return this.inp;
	}
}
