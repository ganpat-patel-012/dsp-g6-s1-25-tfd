# dsp-tfd
Read me

To set up Airflot UI on browser Follow the following steps:


Airflow UI connection on browser

Step 1:

http://localhost:8080
Click on "Admin" → "Connections".
Look for a connection named tfd_db.
If tfd_db is missing, proceed to Step 2.

Step 2: Manually Add the    tfd_db     Connection

In Airflow UI, go to Admin → Connections.
Click " (Add Connection)".

Step 3: 
	
Connection Id:	tfd_db
Connection Type:	Postgres
Host:	tfd_postgres
Schema/Database:	tfd_db
Login:	tfd_user
Password:	tfd_pass
Port:	5432
4.Click "Save" 

