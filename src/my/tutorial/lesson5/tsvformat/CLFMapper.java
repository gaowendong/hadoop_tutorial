package my.tutorial.lesson5.tsvformat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CLFMapper extends Mapper<Object, Text, Text, Text> {
	
	static final Log LOG = LogFactory.getLog(CLFMapper.class);
	
	private SimpleDateFormat dataFormatter = 
			new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    private Pattern p = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" (\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", Pattern.DOTALL);

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	protected void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String entry = value.toString();
		
		//LOG.info("value is " + entry);
		LOG.info("pattern is " + p.pattern());
		
		Matcher m = p.matcher(entry);
		
		if(!m.matches()){
			LOG.info("quite for no matching");
			return;
		}
		
		Date date = null;
		try{
			date = dataFormatter.parse(m.group(4));
		}catch(ParseException ex){
			return;
		}
		
		outputKey.set(m.group(1));//ip as key
		StringBuilder b = new StringBuilder();
		b.append(date.getTime());//timestamp
		b.append('\t');
		b.append(m.group(6));//page
		b.append('\t');
		b.append(m.group(8));//http status
		b.append('\t');
		b.append(m.group(9));//bytes
		b.append('\t');
		b.append(m.group(12));//useragent
		outputValue.set(b.toString());//set assmble string as value
		context.write(outputKey, outputValue);
	}

}
