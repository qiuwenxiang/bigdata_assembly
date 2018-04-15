package provider;

import consumer.DemoService;

public class DemoServiceImpl implements DemoService {
    public String sayHello(String name) {
        return "Hello1 "+name;
    }

}
