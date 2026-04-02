package edu.upenn.cis.orchestra.repository.utils.loader;

import java.io.FileOutputStream;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

/**
 * Schema loader to be called from a console.
 * 
 * @author Sam Donnelly
 */
public class SchemaLoaderCLI {

	/**
	 * 
	 * @param args
	 *            <p>
	 *            <code>[0]</code> the name of the bean to use in
	 *            "edu/upenn/cis/orchestra/repository/utils/loader/SpringConfig.xml"
	 *            .
	 *            <p>
	 *            <code>[1]</code> the output file. Where to write the Orchestra
	 *            schema.
	 */
	public static void main(String[] args) throws Throwable {
		String beanId = args[0], outputFile = args[1];
		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"edu/upenn/cis/orchestra/repository/utils/loader/SpringConfig.xml");

		DataSource ds = (DataSource) ctx.getBean(beanId);
		ISchemaLoader loader = new SchemaLoaderJdbc(ds,
				new SchemaLoaderConfig());
		OrchestraSystem orchestraSystem = loader.buildSystem(outputFile
				.substring(0, outputFile.indexOf('.')), null, null, null);
		orchestraSystem.serialize(new FileOutputStream(outputFile));
	}
}
