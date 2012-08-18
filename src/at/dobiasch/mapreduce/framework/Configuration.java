package at.dobiasch.mapreduce.framework;

public class Configuration
{
	private String indir, outdir;
	private String mapper, reducer;
	
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
}
