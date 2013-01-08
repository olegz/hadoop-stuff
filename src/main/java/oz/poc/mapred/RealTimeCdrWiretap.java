package oz.poc.mapred;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.filter.ExpressionEvaluatingSelector;
import org.springframework.integration.router.RecipientListRouter;
import org.springframework.integration.router.RecipientListRouter.Recipient;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * 
 * @author Oleg Zhurakousky
 *
 */
public class RealTimeCdrWiretap{
	
	private final EventDrivenConsumer rlrConsumer;

	public RealTimeCdrWiretap(EventDrivenConsumer rlrConsumer){
		this.rlrConsumer = rlrConsumer;
	}
	
	public void registerInterest(byte[] queryBytes) throws Exception{
		String query = new String(queryBytes);
		String[] split = StringUtils.split(query," ");
		String regex = split[0];
		String address = split[1];
		
		String[] addressSplit = StringUtils.split(address, ":");
		String host = addressSplit[0];
		int port = Integer.parseInt(addressSplit[1]);
		
		//this.startListener(host, port);
		
		List<Recipient> recipients = this.obtainRecipientList();
		
		Recipient recipient = this.createRecipient(host, port, regex);
		recipients.add(recipient);
		
	}
	
//	private void startListener(String host, int port){
//		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
//		context.load("classpath:/oz/poc/client/config.xml");
//		Map<String, Object> configurator = new HashMap<String, Object>();
//		configurator.put("host", host);
//		configurator.put("port", port);
//		context.getBeanFactory().registerSingleton("configurator", configurator);
//		context.refresh();
//	}
	
	private Recipient createRecipient(String host, int port, String regex){
		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load("classpath:/oz/poc/mapred/wiretap-config-template.xml");
		Map<String, Object> configurator = new HashMap<String, Object>();
		configurator.put("host", host);
		configurator.put("port", port);
		context.getBeanFactory().registerSingleton("configurator", configurator);
		context.refresh();
		MessageChannel wireTapChannel = context.getBean("wiretapChannel", MessageChannel.class);
		ExpressionEvaluatingSelector selector = new ExpressionEvaluatingSelector("payload matches '" + regex + "'");
		Recipient recipient = new Recipient(wireTapChannel, selector);
		return recipient;
		
	}
	
	@SuppressWarnings("unchecked")
	private List<Recipient> obtainRecipientList(){
		
		Field field = ReflectionUtils.findField(EventDrivenConsumer.class, "handler");
		field.setAccessible(true);
		RecipientListRouter rlr = (RecipientListRouter) ReflectionUtils.getField(field, rlrConsumer);
		
		Field rf = ReflectionUtils.findField(RecipientListRouter.class, "recipients");
		rf.setAccessible(true);
		List<Recipient> recipients = (List<Recipient>) ReflectionUtils.getField(rf, rlr);
		
		return recipients;
	}

}
