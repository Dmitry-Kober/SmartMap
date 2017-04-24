package org.smartsoftware.request.manager;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by Dmitry on 23.04.2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:test-context.xml"})
public class HashBasedRequestManagerTest {

    @Autowired
    private HashBasedRequestManager requestManager;

    @Test
    public void testInitializationProcess() {
        requestManager.test();
    }
}
