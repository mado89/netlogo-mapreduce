package org.nlogo.extensions.mapreduce.commands;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;
import org.nlogo.api.Syntax;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.LogoObject;
import at.dobiasch.mapreduce.framework.RecordReader;
import at.dobiasch.mapreduce.framework.RecordWriter;
import at.dobiasch.mapreduce.framework.controller.Data;
import at.dobiasch.mapreduce.framework.controller.HostController;

public class Result extends DefaultReporter
{
	private double val;
	
	public Syntax getSyntax()
	{
		return Syntax.reporterSyntax( new int[] { Syntax.NumberType() } , Syntax.ListType() ) ;
	}
	
	private boolean isNumeric(String str)
	{
		try {
			val= Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}
	
	public Object report(Argument args[], Context context)
			throws ExtensionException, LogoException
	{
		int nr;
		try {
			nr= args[0].getIntValue();
		} catch (LogoException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
		
		Framework fw= FrameworkFactory.getInstance();
		String outdir= fw.getConfiguration().getOutputDirectory();
		File outputdir = new File(outdir);
		if( !outputdir.isAbsolute() )
		{
			outdir= fw.getConfiguration().getBaseDir() + outdir;
			outputdir= new File(outdir);
		}
		
		RecordReader in;
		
		try {
			
			in = new RecordReader(String.format(outdir + "output-%02d.txt", nr), ": ", false);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
		
		LogoListBuilder lb= new LogoListBuilder();
		
		while(in.hasRecordsLeft())
		{
			String[] h= in.readRecord();
			
			/*
			System.out.print("Record: ");
			for(String xx : h)
				System.out.print(xx + " ");
			System.out.println();
			*/
			
			LogoListBuilder kvpair= new LogoListBuilder();
			
			LogoObject obj= new LogoObject();
			
			// key
			obj.createFromString(h[0]);
			// obj.importValue(h[0]);
			kvpair.add(obj.getObject());
			
			// value
			obj.importValue(h[1]);
			// obj.createFromString(h[1]);
			kvpair.add(obj.getObject());
			
			lb.add(kvpair.toLogoList());
		}
		
		return lb.toLogoList();
	}
}
