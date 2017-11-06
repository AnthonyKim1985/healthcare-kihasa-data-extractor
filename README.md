# healthcare-kihasa-data-extractor

<h3>The healthcare project consists of seven sub-projects and two utility projects.</h3>

![System Overview](https://github.com/AnthonyKim1985/healthcare-integration-platform/blob/master/image/system_architecture.png)
* <i>The authentication functionality has not been implemented yet.</i>
* <i>The Hadoop ecosystem can be physically separated for each institution. However, in this implementation I implemented it with one cluster.</i>
<h4>(1) sub project</h4><br>
<h6>1) healthcare-integration-platform<br>
Upon receiving the request from the user, the requested data is parsed by referring to the meta database. If it is a suitable request, it queues the job to the queuing system (kafka or rabbitmq).</h6><br>
<h6>2) healthcare-task-dispatcher<br>The client's request is stored in the database. It decides which organization is to be allocated this request, and distributes the task at regular intervals.</h6><br>
<h6>3) healthcare-cdc-data-extractor<br>Middleware for extracting data from KCDC (Korea Centers for Disease Control & Prevention) organization. It provides the function to extract data corresponding to user's request such as KCDC's community health survey, national health nutrition survey, and genome data.</h6><br>
<h6>4) healthcare-nhic-data-extractor<br>Middleware for extracting data from NHIC (National Health Insurence Center) organization. It provides the function to extract data corresponding to user's request such as NHIC's disease, treatment materials, medical activities, medicine and patient standard data.</h6><br>
<h6>5) healthcare-hira-data-extractor<br>Middleware for extracting data from HIRA (Health Insurence Review & Assessment service) organization. It is based on patient, hospitalized patient, hospitalized patient, pediatric and elderly patient data based on patient and specification standards such as diseases, And provides a function of extracting data corresponding to a user's request.</h6><br>
<h6>6) healthcare-kihasa-data-extractor<br>Middleware for extracting data from KIHASA(Korea Institute for Health and Social Affairs) organization. It provides the function to extract data corresponding to user's request according to meaningful indicators based on KIHASA's medical panel data.</h6><br>
<h6>7) healthcare-scenario-processor<br>It provides data extraction function for WorkFlow UI.</h6><br>

<h4>(2) utility</h4><br>
<h6>1) healthcare-meta-data-refinery<br>It is a utility that distinguishes among the columns that are unique to columns of each dataset and those that are not.</h6><br>
<h6>2) hive-table-maker<br>Dynamically generate a hive schema suitable for a dataset with datasets as input values.</h6><br>
