``` 
   <bean name="elasticsearchTemplate" class="org.springframework.data.elasticsearch.core.ElasticsearchTemplate">
        <constructor-arg name="client" ref="client"/>
    </bean>
    
    <elasticsearch:transport-client id="client" cluster-name="${elasticsearch.clustername}" cluster-nodes="${elasticsearch.nodes}" client-transport-sniff="false"/>
    
    <elasticsearch:repositories base-package="com.lenovo.caleido.search.dao" elasticsearch-template-ref="elasticsearchTemplate"/>

```
  
        
    