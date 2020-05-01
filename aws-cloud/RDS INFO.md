# **RDS(relational database service)**

* RDS in aws is the database as a service.

* RDS will give you not only the mysql but also sqlserver,oracle,aurora mysql.


## STEPS TO MADE INSTANCE(MYSQL):-

### step 1:Click on services.

### step 2:Search for RDS and click RDS

### step 3:Click on create database.

### step 4:Click on Easy create.

### step 5:Click MYSQL.

### step 6:Select version.

### step 7:In templates click free tier.

### step 8:Give instance name.

### step 9:Give username and password u should compusory know to connect the local host so it should be recorded.

### step 10:DB instance type select db.t2.micro only.

### step 11:In connectivity go to addiitonal connectivity configuration and select public accessible yes.

### step 12:last option create database.

### step 13:-wait till it creates RDS

### step 14:-See the details below see port number we have to add that port number to inbound rules.

### step 15:-see right default security right click and open new tab.

### step 16:-Dont touch the default setting click on edit inbound rules add port number and then select anywhere.

### step 17:-Go to workbench.

### step 18:-new connection,enter connection name.

### step 19:-enter hostname that is go to aws site see the endpoint copy paste that url to hostname in workbench.

### step 20:-username u should give that u created.

### step 21:-Enter password and then check and finally create.
