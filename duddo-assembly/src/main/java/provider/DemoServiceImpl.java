package provider;

import consumer.DemoService;

public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        return "Hello1 "+name;
    }

}
