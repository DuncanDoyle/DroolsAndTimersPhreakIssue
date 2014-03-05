package org.jboss.ddoyle.drools.sample;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.drools.core.time.impl.PseudoClockScheduler;
import org.junit.Assert;
import org.junit.Test;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.Environment;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.api.runtime.conf.KieSessionOption;
import org.kie.api.runtime.conf.TimedRuleExectionOption;
import org.kie.api.time.SessionClock;
import org.kie.internal.KnowledgeBase;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderConfiguration;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.builder.conf.RuleEngineOption;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.runtime.StatefulKnowledgeSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests timers in the PHREAK engine.
 * <p/>
 * Note that this test is quite dirty, as it uses Thread.sleep statements to wait for all the rules/matches to have fired. Better would be to introduce some
 * form of synchronization, but I was lazy. 
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class TimerTest extends Assert {

	private final static RuleEngineOption phreak = RuleEngineOption.PHREAK;

	private static final Logger LOGGER = LoggerFactory.getLogger(TimerTest.class);

	
	/* 
	 * This one times out because the rules is never fired, so it hangs on the barrier.
	 */
	@Test(timeout=5000)
	public void testOne() throws Exception {

		KieSession ksession = getKieSession();
		
        List list = new ArrayList();

        PseudoClockScheduler timeService = ( PseudoClockScheduler ) ksession.<SessionClock>getSessionClock();
        timeService.advanceTime( new Date().getTime(), TimeUnit.MILLISECONDS );

        ksession.setGlobal( "list", list );
		
		// This is required in Phreak to kickstart the timers.
		ksession.fireAllRules();
		ksession.insert(new SimpleEvent());
		assertEquals(0, list.size());
		
		// Advance the time .... so the timer will fire.
		timeService.advanceTime(10000, TimeUnit.MILLISECONDS);
		Thread.sleep(1000);
        assertEquals(1, list.size());

		int counter;
		for (counter = 0; counter < 5; counter++) {
			ksession.insert(new SimpleEvent());
			//ksession.fireAllRules();
		}
		LOGGER.info("And out of loop.");
		LOGGER.info("Advancing time with 10 seconds.");
		timeService.advanceTime(10000, TimeUnit.MILLISECONDS);
		Thread.sleep(1000);
        assertEquals(7, list.size());
		
		ksession.dispose();
	}
	
	/* 
	 * This one fails as well. As we've inserted 6 events, I expected the rule to be called 6 times on the second timer fire.
	 * So, that should make 7 fires in total. Instead, the rule is only fired twice.
	 * 
	 * The difference with testOne() is that I insert the SimpleEvent BEFORE I call ksession.fireAllRules().
	 */
	@Test(timeout=5000)
	public void testTwo() throws Exception {

		KieSession ksession = getKieSession();
		
		List list = new ArrayList();

        PseudoClockScheduler timeService = ( PseudoClockScheduler ) ksession.<SessionClock>getSessionClock();
        timeService.advanceTime( new Date().getTime(), TimeUnit.MILLISECONDS );

        ksession.setGlobal( "list", list );
		
		// This is required in Phreak to kickstart the timers.
        ksession.insert(new SimpleEvent());
		ksession.fireAllRules();
		assertEquals(0, list.size());
		
		// Advance the time .... so the timer will fire.
		timeService.advanceTime(10000, TimeUnit.MILLISECONDS);
		Thread.sleep(1000);
        assertEquals(1, list.size());

		int counter;
		for (counter = 0; counter < 5; counter++) {
			ksession.insert(new SimpleEvent());
			//ksession.fireAllRules();
		}
		LOGGER.info("And out of loop.");
		LOGGER.info("Advancing time with 10 seconds.");
		timeService.advanceTime(10000, TimeUnit.MILLISECONDS);
		Thread.sleep(1000);
        assertEquals(7, list.size());
		
		ksession.dispose();
	}
	
	/* 
	 * This test succeeds. The big difference with the other tests is that I explicitly call 'fireAllRules' after I insert the events/facts.
	 * 
	 */
	@Test(timeout=5000)
	public void testThree() throws Exception {

		KieSession ksession = getKieSession();
		
		List list = new ArrayList();

        PseudoClockScheduler timeService = ( PseudoClockScheduler ) ksession.<SessionClock>getSessionClock();
        timeService.advanceTime( new Date().getTime(), TimeUnit.MILLISECONDS );

        ksession.setGlobal( "list", list );
		
		// This is required in Phreak to kickstart the timers.
        ksession.insert(new SimpleEvent());
		ksession.fireAllRules();
		assertEquals(0, list.size());
		
		// Advance the time .... so the timer will fire.
		timeService.advanceTime(10000, TimeUnit.MILLISECONDS);
		Thread.sleep(1000);
		
		assertEquals(1, list.size());

		int counter;
		for (counter = 0; counter < 5; counter++) {
			ksession.insert(new SimpleEvent());
			ksession.fireAllRules();
		}
		LOGGER.info("And out of loop.");
		LOGGER.info("Advancing time with 10 seconds.");
		timeService.advanceTime(10000, TimeUnit.MILLISECONDS);
		//ksession.fireAllRules();
		Thread.sleep(1000);
        assertEquals(7, list.size());
		
		ksession.dispose();
	}
	
	
	private KieSession getKieSession() {
		String str = "package org.jboss.ddoyle.drools.sample \n" +
				"global java.util.List list \n" +
				"rule xxx \n" + 
						"  timer (int:10s 10s) " + 
				"when \n" +
					"$s: SimpleEvent() \n" + 
				"then \n" + 
						"  list.add(\"fired\"); \n" +
						"  System.out.println(\"Simple Event\"); \n" + 
				"end  \n";

		KieSessionConfiguration conf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
				
		conf.setOption( ClockTypeOption.get( "pseudo" ) );
		conf.setOption( TimedRuleExectionOption.YES );

		KnowledgeBase kbase = loadKnowledgeBaseFromString(str);
		return createKnowledgeSession(kbase, conf);
	}
	
	
	
	

	protected KnowledgeBase loadKnowledgeBaseFromString(String... drlContentStrings) {
		return loadKnowledgeBaseFromString(null, null, phreak, drlContentStrings);
	}

	protected KnowledgeBase loadKnowledgeBaseFromString(RuleEngineOption phreak, String... drlContentStrings) {
		return loadKnowledgeBaseFromString(null, null, phreak, drlContentStrings);
	}

	protected KnowledgeBase loadKnowledgeBaseFromString(KnowledgeBuilderConfiguration config, String... drlContentStrings) {
		return loadKnowledgeBaseFromString(config, null, phreak, drlContentStrings);
	}

	protected KnowledgeBase loadKnowledgeBaseFromString(KieBaseConfiguration kBaseConfig, String... drlContentStrings) {
		return loadKnowledgeBaseFromString(null, kBaseConfig, phreak, drlContentStrings);
	}

	protected KnowledgeBase loadKnowledgeBaseFromString(KnowledgeBuilderConfiguration config, KieBaseConfiguration kBaseConfig,
			RuleEngineOption phreak, String... drlContentStrings) {
		KnowledgeBuilder kbuilder = config == null ? KnowledgeBuilderFactory.newKnowledgeBuilder() : KnowledgeBuilderFactory
				.newKnowledgeBuilder(config);
		for (String drlContentString : drlContentStrings) {
			kbuilder.add(ResourceFactory.newByteArrayResource(drlContentString.getBytes()), ResourceType.DRL);
		}

		if (kbuilder.hasErrors()) {
			fail(kbuilder.getErrors().toString());
		}
		if (kBaseConfig == null) {
			kBaseConfig = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
		}
		kBaseConfig.setOption(phreak);
		KnowledgeBase kbase = kBaseConfig == null ? KnowledgeBaseFactory.newKnowledgeBase() : KnowledgeBaseFactory
				.newKnowledgeBase(kBaseConfig);
		kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());
		return kbase;
	}

	protected StatefulKnowledgeSession createKnowledgeSession(KnowledgeBase kbase) {
		return kbase.newStatefulKnowledgeSession();
	}

	protected StatefulKnowledgeSession createKnowledgeSession(KnowledgeBase kbase, KieSessionOption option) {
		KieSessionConfiguration ksconf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
		ksconf.setOption(option);
		return kbase.newStatefulKnowledgeSession(ksconf, null);
	}

	protected StatefulKnowledgeSession createKnowledgeSession(KnowledgeBase kbase, KieSessionConfiguration ksconf) {
		return kbase.newStatefulKnowledgeSession(ksconf, null);
	}

	protected static StatefulKnowledgeSession createKnowledgeSession(KnowledgeBase kbase, KieSessionConfiguration ksconf, Environment env) {
		return kbase.newStatefulKnowledgeSession(ksconf, env);
	}
}
